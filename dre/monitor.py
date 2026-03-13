"""
Data Reliability Engineer — Continuous Multi-Job Monitor
=========================================================
Automatically discovers ALL AWS Glue jobs and monitors each one
independently with its own alert chain, state tracker, and failure context.

Each job gets:
  - Its own poll loop (detects new completions independently)
  - Its own freshness check schedule
  - Its own schema + data drift schedule
  - Its own email alert (never grouped with other jobs)

Run:
    python monitor.py
"""

import json
import logging
import os
import time
import threading

import boto3
import yaml
from dotenv import load_dotenv

from checks.job_health import run_job_health_check
from checks.data_quality import run_all_quality_checks, run_freshness_check
from checks.schema_drift import run_schema_drift_check
from checks.data_drift import run_data_drift_check
from alerting.email_alert import build_report, send_alert_email, should_alert

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def load_config() -> dict:
    path = os.path.join(os.path.dirname(__file__), "config", "pipelines.yaml")
    with open(path) as f:
        return yaml.safe_load(f)


def get_aws_clients():
    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )
    return session.client("glue"), session.client("logs")


def get_snowflake_conn():
    import snowflake.connector
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "ANALYTICS"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RETAIL"),
    )


# ── Config helpers ────────────────────────────────────────────────────────────

def discover_all_jobs(glue_client) -> list:
    jobs = []
    kwargs = {}
    while True:
        resp = glue_client.list_jobs(**kwargs)
        jobs.extend(resp.get("JobNames", []))
        token = resp.get("NextToken")
        if not token:
            break
        kwargs["NextToken"] = token
    return jobs


def build_job_config(glue_job_name: str, cfg: dict) -> dict:
    defaults = cfg.get("defaults", {})
    explicit = next(
        (j for j in cfg.get("jobs", []) if j["glue_job_name"] == glue_job_name),
        None,
    )
    if explicit:
        return explicit
    # Auto-generated config for undeclared jobs
    return {
        "name": glue_job_name,
        "glue_job_name": glue_job_name,
        "sla_runtime_minutes": defaults.get("sla_runtime_minutes", 60),
        "freshness": {
            "key_column": defaults.get("freshness_key_column", "DATE"),
            "max_delay_minutes": defaults.get("freshness_max_delay_minutes", 120),
        },
        "row_count": {"deviation_threshold_pct": defaults.get("row_count_deviation_pct", 20)},
        "target_tables": [],
    }


def get_table_configs(job_cfg: dict) -> dict:
    return {t["name"]: t for t in job_cfg.get("target_tables", [])}


# ── Alert dispatcher ──────────────────────────────────────────────────────────

def fire_alert(job_cfg, job_result, quality_results, schema_results, drift_results, logger):
    report = build_report(job_cfg, job_result, quality_results, schema_results, drift_results)
    if not should_alert(report):
        logger.info("✅ All checks passed — no alert needed")
        return
    logger.warning(
        f"🚨 {report['email_subject']} | "
        f"{len(report['issues_detected'])} issue(s) | "
        f"Confidence: {report['confidence']}"
    )
    sent = send_alert_email(
        report,
        gmail_user=os.getenv("GMAIL_USER"),
        gmail_app_pass=os.getenv("GMAIL_APP_PASS"),
        to_email=os.getenv("ALERT_EMAIL", "itsnishant56@gmail.com"),
    )
    if sent:
        logger.info(f"📧 Sent: {report['email_subject']}")
    else:
        logger.error(f"❌ Email failed: {report['email_subject']}")


# ── Per-job check runners ─────────────────────────────────────────────────────

def run_full_analysis(glue_client, logs_client, job_cfg, logger):
    job_result = run_job_health_check(glue_client, logs_client, job_cfg)
    logger.info(
        f"Status: {job_result.get('status')} | "
        f"Duration: {job_result.get('duration_minutes')}m | "
        f"Failure: {job_result.get('failure_type') or 'none'}"
    )
    table_cfgs = get_table_configs(job_cfg)
    quality_results, schema_results, drift_results = [], [], []
    if table_cfgs:
        try:
            sf = get_snowflake_conn()
            for table, tcfg in table_cfgs.items():
                quality_results.append(run_all_quality_checks(sf, table, tcfg, job_cfg))
                schema_results.append(run_schema_drift_check(sf, table))
                drift_results.append(run_data_drift_check(sf, table, tcfg))
            sf.close()
        except Exception as e:
            logger.warning(f"Snowflake checks skipped: {e}")
    fire_alert(job_cfg, job_result, quality_results, schema_results, drift_results, logger)


def run_freshness_check_job(job_cfg, logger):
    freshness_cfg = job_cfg.get("freshness", {})
    table_cfgs = get_table_configs(job_cfg)
    if not table_cfgs:
        return
    all_freshness_issues = []
    quality_results = []
    try:
        sf = get_snowflake_conn()
        for table in table_cfgs:
            fr = run_freshness_check(
                sf, table,
                freshness_cfg.get("key_column", "DATE"),
                freshness_cfg.get("max_delay_minutes", 120),
            )
            if not fr["passed"]:
                logger.warning(f"FRESHNESS BREACH: {table}")
                all_freshness_issues.extend(fr["issues"])
                quality_results.append({"table": table, "all_issues": fr["issues"]})
        sf.close()
    except Exception as e:
        logger.warning(f"Freshness check skipped: {e}")
        return
    if all_freshness_issues:
        dummy = {"status": "FRESHNESS_BREACH", "failure_type": None,
                 "error_message": "", "remediation": [], "runtime_sla": {}}
        fire_alert(job_cfg, dummy, quality_results, [], [], logger)


def run_drift_check_job(job_cfg, logger):
    table_cfgs = get_table_configs(job_cfg)
    if not table_cfgs:
        return
    schema_results, drift_results = [], []
    try:
        sf = get_snowflake_conn()
        for table, tcfg in table_cfgs.items():
            schema_results.append(run_schema_drift_check(sf, table))
            drift_results.append(run_data_drift_check(sf, table, tcfg))
        sf.close()
    except Exception as e:
        logger.warning(f"Drift check skipped: {e}")
        return
    has_issues = (
        any(r.get("drift_detected") for r in schema_results) or
        any(not r.get("passed", True) for r in drift_results)
    )
    if has_issues:
        dummy = {"status": "DRIFT_DETECTED", "failure_type": None,
                 "error_message": "", "remediation": [], "runtime_sla": {}}
        fire_alert(job_cfg, dummy, [], schema_results, drift_results, logger)
    else:
        logger.info("Drift check: ✅ No drift detected")


# ── Per-job monitor thread ────────────────────────────────────────────────────

class JobMonitor(threading.Thread):
    """One independent thread per Glue job."""

    def __init__(self, glue_job_name, job_cfg, glue_client, logs_client, settings):
        super().__init__(name=f"Monitor-{glue_job_name}", daemon=True)
        self.glue_job_name   = glue_job_name
        self.job_cfg         = job_cfg
        self.glue_client     = glue_client
        self.logs_client     = logs_client
        self.settings        = settings
        self.logger          = logging.getLogger(f"DRE.{glue_job_name}")
        self._last_run_id    = None
        self._last_fresh_ts  = 0.0
        self._last_drift_ts  = 0.0

    def run(self):
        poll      = self.settings.get("poll_interval_seconds", 60)
        fresh_int = self.settings.get("freshness_check_interval_seconds", 300)
        drift_int = self.settings.get("drift_check_interval_seconds", 3600)

        self.logger.info(f"▶ Started | poll={poll}s | freshness={fresh_int}s | drift={drift_int}s")

        while True:
            now = time.time()

            # Job completion check
            try:
                is_new, run_info = self._check_completion()
                if is_new:
                    state = run_info.get("JobRunState", "UNKNOWN") if run_info else "UNKNOWN"
                    self.logger.info(f"🔔 New completion: {state}")
                    run_full_analysis(
                        self.glue_client, self.logs_client,
                        self.job_cfg, self.logger,
                    )
            except Exception as e:
                self.logger.error(f"Completion check error: {e}")

            # Freshness check
            if now - self._last_fresh_ts >= fresh_int:
                try:
                    run_freshness_check_job(self.job_cfg, self.logger)
                except Exception as e:
                    self.logger.error(f"Freshness error: {e}")
                self._last_fresh_ts = now

            # Drift check
            if now - self._last_drift_ts >= drift_int:
                try:
                    run_drift_check_job(self.job_cfg, self.logger)
                except Exception as e:
                    self.logger.error(f"Drift error: {e}")
                self._last_drift_ts = now

            time.sleep(poll)

    def _check_completion(self) -> tuple:
        resp = self.glue_client.get_job_runs(JobName=self.glue_job_name, MaxResults=1)
        runs = resp.get("JobRuns", [])
        if not runs:
            return False, None
        run   = runs[0]
        run_id = run["Id"]
        state  = run["JobRunState"]
        if state in ("RUNNING", "STARTING", "STOPPING", "WAITING"):
            return False, None
        if self._last_run_id == run_id:
            return False, None
        self._last_run_id = run_id
        return True, run


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    logger = logging.getLogger("DRE")
    logger.info("=" * 65)
    logger.info("🔍 Data Reliability Engineer — Continuous Multi-Job Monitor")
    logger.info("=" * 65)

    cfg            = load_config()
    settings       = cfg.get("settings", {})
    glue_client, logs_client = get_aws_clients()

    all_jobs = discover_all_jobs(glue_client)
    logger.info(f"Discovered {len(all_jobs)} Glue job(s): {', '.join(all_jobs)}")

    threads   = []
    known     = set()

    for name in all_jobs:
        job_cfg = build_job_config(name, cfg)
        t = JobMonitor(name, job_cfg, glue_client, logs_client, settings)
        t.start()
        threads.append(t)
        known.add(name)
        logger.info(f"  ▶ Monitor started: {name}")

    logger.info(f"\n{len(threads)} job monitor(s) running. Press Ctrl+C to stop.\n")

    # Re-discover newly created Glue jobs every 10 min
    while True:
        time.sleep(600)
        try:
            current = set(discover_all_jobs(glue_client))
            for name in current - known:
                logger.info(f"🆕 New Glue job discovered: {name}")
                job_cfg = build_job_config(name, cfg)
                t = JobMonitor(name, job_cfg, glue_client, logs_client, settings)
                t.start()
                threads.append(t)
                known.add(name)
        except Exception as e:
            logger.error(f"Re-discovery error: {e}")


if __name__ == "__main__":
    main()
