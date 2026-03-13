"""
Data Reliability Engineer — Automated Monitor Daemon
=====================================================
Runs continuously and automatically:
  • Polls Glue job status every 60s → triggers full DRE check on completion
  • Runs freshness checks every 5 min
  • Runs schema drift + data drift checks every 1 hour
  • Sends email alerts automatically when issues detected

Run:
    python monitor.py
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import boto3
import snowflake.connector
import yaml
from dotenv import load_dotenv

from checks.job_health import run_job_health_check
from checks.data_quality import run_all_quality_checks
from checks.schema_drift import run_schema_drift_check
from checks.data_drift import run_data_drift_check
from alerting.email_alert import build_report, send_alert_email, should_alert

# ── Setup ────────────────────────────────────────────────────────────────────

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("DRE")


def load_config() -> dict:
    path = os.path.join(os.path.dirname(__file__), "config", "pipelines.yaml")
    with open(path) as f:
        return yaml.safe_load(f)


# ── AWS + Snowflake clients ───────────────────────────────────────────────────

def get_aws_clients():
    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )
    return session.client("glue"), session.client("logs")


def get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "ANALYTICS"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RETAIL"),
    )


# ── Core DRE analysis ─────────────────────────────────────────────────────────

def run_full_dre_analysis(pipeline_cfg: dict, glue_client, logs_client, table_configs: dict):
    """
    Run all DRE checks for a pipeline and send alert if issues found.
    """
    job_name = pipeline_cfg["name"]
    logger.info(f"[{job_name}] Starting full DRE analysis...")

    # 1. Job health check
    job_result = run_job_health_check(glue_client, logs_client, pipeline_cfg)
    logger.info(f"[{job_name}] Job status: {job_result.get('status')}")

    # 2. Connect to Snowflake for data checks
    sf_conn = None
    quality_results = []
    schema_results = []
    drift_results = []

    try:
        sf_conn = get_snowflake_conn()

        for table in pipeline_cfg.get("target_tables", []):
            table_cfg = table_configs.get(table, {})
            logger.info(f"[{job_name}] Running checks on {table}...")

            # Data quality
            qr = run_all_quality_checks(sf_conn, table, table_cfg, pipeline_cfg)
            quality_results.append(qr)
            if not qr["passed"]:
                logger.warning(f"[{job_name}] {table}: {qr['total_issues']} quality issue(s)")

            # Schema drift
            sr = run_schema_drift_check(sf_conn, table)
            schema_results.append(sr)
            if sr.get("drift_detected"):
                logger.warning(f"[{job_name}] {table}: Schema drift detected!")

            # Data drift
            dr = run_data_drift_check(sf_conn, table, table_cfg)
            drift_results.append(dr)
            if not dr["passed"]:
                logger.warning(f"[{job_name}] {table}: Data drift detected!")

    except Exception as e:
        logger.error(f"[{job_name}] Snowflake connection/check failed: {e}")
    finally:
        if sf_conn:
            sf_conn.close()

    # 3. Build report
    report = build_report(pipeline_cfg, job_result, quality_results, schema_results, drift_results)

    # 4. Log JSON report
    logger.info(f"[{job_name}] Report:\n{json.dumps(report, indent=2)}")

    # 5. Send alert if warranted
    if should_alert(report):
        sent = send_alert_email(
            report,
            gmail_user=os.getenv("GMAIL_USER"),
            gmail_app_pass=os.getenv("GMAIL_APP_PASS"),
            to_email=os.getenv("ALERT_EMAIL"),
        )
        if sent:
            logger.info(f"[{job_name}] 📧 Alert sent: {report['email_subject']}")
    else:
        logger.info(f"[{job_name}] ✅ All checks passed — no alert needed")

    return report


def run_freshness_only(pipeline_cfg: dict, table_configs: dict):
    """Lightweight freshness check (runs every 5 min)."""
    job_name = pipeline_cfg["name"]
    freshness_cfg = pipeline_cfg.get("freshness", {})
    if not freshness_cfg:
        return

    from checks.data_quality import run_freshness_check

    try:
        sf_conn = get_snowflake_conn()
        for table in pipeline_cfg.get("target_tables", []):
            fr = run_freshness_check(
                sf_conn, table,
                freshness_cfg.get("key_column", "DATE"),
                freshness_cfg.get("max_delay_minutes", 60),
            )
            if not fr["passed"]:
                logger.warning(f"[{job_name}] FRESHNESS BREACH: {fr['issues']}")
                # Build minimal report for freshness-only alert
                dummy_job = {"status": "FRESHNESS_BREACH", "remediation": [], "runtime_sla": {}}
                report = build_report(pipeline_cfg, dummy_job, [{"all_issues": fr["issues"]}], [], [])
                if should_alert(report):
                    send_alert_email(
                        report,
                        gmail_user=os.getenv("GMAIL_USER"),
                        gmail_app_pass=os.getenv("GMAIL_APP_PASS"),
                        to_email=os.getenv("ALERT_EMAIL"),
                    )
        sf_conn.close()
    except Exception as e:
        logger.error(f"[{job_name}] Freshness check error: {e}")


def run_drift_only(pipeline_cfg: dict, table_configs: dict, glue_client, logs_client):
    """Schema + data drift check (runs every hour)."""
    job_name = pipeline_cfg["name"]
    try:
        sf_conn = get_snowflake_conn()
        schema_results = []
        drift_results = []

        for table in pipeline_cfg.get("target_tables", []):
            table_cfg = table_configs.get(table, {})
            sr = run_schema_drift_check(sf_conn, table)
            schema_results.append(sr)
            dr = run_data_drift_check(sf_conn, table, table_cfg)
            drift_results.append(dr)

        sf_conn.close()

        all_issues = []
        for r in schema_results + drift_results:
            all_issues.extend(r.get("issues", []))

        if all_issues:
            dummy_job = {"status": "DRIFT_DETECTED", "remediation": [], "runtime_sla": {}}
            report = build_report(pipeline_cfg, dummy_job, [], schema_results, drift_results)
            if should_alert(report):
                logger.warning(f"[{job_name}] Drift alert: {len(all_issues)} issue(s)")
                send_alert_email(
                    report,
                    gmail_user=os.getenv("GMAIL_USER"),
                    gmail_app_pass=os.getenv("GMAIL_APP_PASS"),
                    to_email=os.getenv("ALERT_EMAIL"),
                )
        else:
            logger.info(f"[{job_name}] Drift check: ✅ No drift detected")

    except Exception as e:
        logger.error(f"[{job_name}] Drift check error: {e}")


# ── Job state tracker ─────────────────────────────────────────────────────────

class JobStateTracker:
    """Tracks last seen Glue job run ID to detect new completions."""

    def __init__(self):
        self._last_run_id: dict[str, str] = {}
        self._last_status: dict[str, str] = {}

    def has_new_completion(self, glue_client, glue_job_name: str) -> tuple[bool, dict | None]:
        """
        Check if a Glue job has a new run that just completed (SUCCEEDED or FAILED).
        Returns (is_new_completion, run_info).
        """
        try:
            resp = glue_client.get_job_runs(JobName=glue_job_name, MaxResults=1)
            runs = resp.get("JobRuns", [])
            if not runs:
                return False, None

            run = runs[0]
            run_id = run["Id"]
            state = run["JobRunState"]

            # Still running
            if state in ("RUNNING", "STARTING", "STOPPING"):
                return False, None

            # Already processed this run
            if self._last_run_id.get(glue_job_name) == run_id:
                return False, None

            # New completion
            self._last_run_id[glue_job_name] = run_id
            self._last_status[glue_job_name] = state
            return True, run

        except Exception as e:
            logger.error(f"Error checking job run for {glue_job_name}: {e}")
            return False, None


# ── Main monitor loop ─────────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("🔍 Data Reliability Engineer Monitor — STARTING")
    logger.info("=" * 60)

    cfg = load_config()
    settings = cfg.get("settings", {})
    pipelines = cfg.get("pipelines", [])
    table_configs = cfg.get("tables", {})

    poll_interval = settings.get("poll_interval_seconds", 60)
    freshness_interval = settings.get("freshness_check_interval_seconds", 300)
    drift_interval = settings.get("drift_check_interval_seconds", 3600)

    glue_client, logs_client = get_aws_clients()
    tracker = JobStateTracker()

    last_freshness_check: dict[str, float] = {}
    last_drift_check: dict[str, float] = {}

    logger.info(f"Monitoring {len(pipelines)} pipeline(s)")
    logger.info(f"Poll interval: {poll_interval}s | Freshness: {freshness_interval}s | Drift: {drift_interval}s")

    while True:
        now = time.time()

        for pipeline in pipelines:
            name = pipeline["name"]
            glue_job = pipeline.get("glue_job_name")

            # ── Job completion check (every poll_interval) ──
            if glue_job:
                new_completion, run_info = tracker.has_new_completion(glue_client, glue_job)
                if new_completion:
                    state = run_info["JobRunState"] if run_info else "UNKNOWN"
                    logger.info(f"[{name}] 🔔 New job completion detected: {state}")
                    run_full_dre_analysis(pipeline, glue_client, logs_client, table_configs)

            # ── Freshness check (every freshness_interval) ──
            if now - last_freshness_check.get(name, 0) >= freshness_interval:
                logger.info(f"[{name}] Running scheduled freshness check...")
                run_freshness_only(pipeline, table_configs)
                last_freshness_check[name] = now

            # ── Drift check (every drift_interval) ──
            if now - last_drift_check.get(name, 0) >= drift_interval:
                logger.info(f"[{name}] Running scheduled drift check...")
                run_drift_only(pipeline, table_configs, glue_client, logs_client)
                last_drift_check[name] = now

        time.sleep(poll_interval)


if __name__ == "__main__":
    main()
