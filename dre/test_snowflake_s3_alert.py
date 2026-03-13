"""
DRE Alert Test — Snowflake table integration failure from AWS S3.

Simulates:
  COPY INTO nishant_test_nonprod_db.json_to_parquet_iceberg
  FROM @NISHANT_TEST_NONPROD_STAGE/raw/
  FILE_FORMAT = (TYPE = PARQUET)

Failure: Snowflake S3 integration IAM role insufficient privileges / stage access denied.

Run: python test_snowflake_s3_alert.py
"""

import json
import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
logger = logging.getLogger("DRE-SNOWFLAKE-TEST")

sys.path.insert(0, os.path.dirname(__file__))

from checks.job_health import classify_failure, check_runtime_sla
from alerting.email_alert import build_report, send_alert_email, should_alert

# ── Simulated Snowflake S3 integration error ──────────────────────────────────

JOB_NAME = "nishant-test-nonprod-json-to-parquet"

# Realistic Snowflake COPY INTO error when S3 integration permissions are wrong
ERROR_MESSAGE = (
    "SQL access control error: Insufficient privileges to operate on stage "
    "'NISHANT_TEST_NONPROD_STAGE'. "
    "Failed to open file: s3://nishant-test-nonprod-source-raw/raw/data.parquet. "
    "AWS credentials not found or SNOWFLAKE_S3_INTEGRATION IAM role "
    "does not have s3:GetObject on bucket 'nishant-test-nonprod-source-raw'. "
    "Please verify STORAGE_AWS_IAM_USER_ARN is trusted in the IAM role trust policy. "
    "Net.snowflake.client.jdbc.SnowflakeSQLException: S3 access denied — "
    "Error copying from S3 stage to Snowflake table json_to_parquet_iceberg."
)

CLOUDWATCH_LOGS = """
[2026-03-13 00:22:01] INFO  Starting Snowflake COPY INTO job
[2026-03-13 00:22:02] INFO  Connecting to Snowflake account: nishant-nonprod
[2026-03-13 00:22:03] INFO  Accessing external stage: NISHANT_TEST_NONPROD_STAGE
[2026-03-13 00:22:04] ERROR SQL access control error: Insufficient privileges to operate on stage 'NISHANT_TEST_NONPROD_STAGE'
[2026-03-13 00:22:04] ERROR Failed to open file: s3://nishant-test-nonprod-source-raw/raw/data.parquet
[2026-03-13 00:22:04] ERROR AWS credentials not found or SNOWFLAKE_S3_INTEGRATION IAM role does not have s3:GetObject
[2026-03-13 00:22:04] ERROR Net.snowflake.client.jdbc.SnowflakeSQLException: S3 access denied
[2026-03-13 00:22:04] ERROR COPY INTO failed — rolling back transaction
[2026-03-13 00:22:05] INFO  Job run marked as FAILED
"""

DURATION_MIN = 0.1

PIPELINE_CFG = {
    "name":          JOB_NAME,
    "glue_job_name": JOB_NAME,
    "target_tables": [
        "nishant_test_nonprod_db.json_to_parquet_iceberg",
        "nishant_test_nonprod_db.json_to_parquet",
    ],
    "freshness": {
        "key_column":        "DATE",
        "max_delay_minutes": 60,
    },
    "row_count": {
        "deviation_threshold_pct": 20,
    },
    "sla_runtime_minutes": 30,
}

# ── Classify failure ──────────────────────────────────────────────────────────

failure_type, remediation = classify_failure(ERROR_MESSAGE, CLOUDWATCH_LOGS)
runtime_sla = check_runtime_sla(DURATION_MIN, PIPELINE_CFG["sla_runtime_minutes"])

logger.info(f"Failure type classified as: {failure_type}")
logger.info(f"Remediation steps ({len(remediation)}):")
for step in remediation:
    logger.info(f"  → {step}")

# ── Build job result ──────────────────────────────────────────────────────────

job_result = {
    "job_name":         PIPELINE_CFG["name"],
    "glue_job":         JOB_NAME,
    "run_id":           "snowflake-s3-integration-test-001",
    "status":           "FAILED",
    "started_at":       "2026-03-13 00:22:01 UTC",
    "completed_at":     "2026-03-13 00:22:05 UTC",
    "duration_minutes": DURATION_MIN,
    "runtime_sla":      runtime_sla,
    "failure_type":     failure_type,
    "error_message":    ERROR_MESSAGE,
    "remediation":      remediation,
    "logs_tail":        CLOUDWATCH_LOGS,
}

# ── Extra data quality issues from integration failure ────────────────────────
# When COPY INTO fails, tables are stale — simulate freshness breach

freshness_issue = {
    "check":    "freshness",
    "severity": "CRITICAL",
    "message":  (
        "nishant_test_nonprod_db.json_to_parquet_iceberg: "
        "Data is 180 min old (SLA: 60 min). "
        "Latest record: 2026-03-12. Table not updated — COPY INTO failed."
    ),
}

null_issue = {
    "check":    "null_validation",
    "severity": "HIGH",
    "message":  (
        "nishant_test_nonprod_db.json_to_parquet: "
        "0 rows loaded — table empty due to failed S3 integration. "
        "All downstream queries will return no data."
    ),
}

quality_results = [{
    "table":       "nishant_test_nonprod_db.json_to_parquet_iceberg",
    "all_issues":  [freshness_issue, null_issue],
    "total_issues": 2,
    "passed":      False,
}]

# ── Build and send report ─────────────────────────────────────────────────────

report = build_report(
    pipeline_cfg    = PIPELINE_CFG,
    job_result      = job_result,
    quality_results = quality_results,
    schema_results  = [],
    drift_results   = [],
)

logger.info(f"\nSeverity  : {report['severity']}")
logger.info(f"Confidence: {report['confidence']}")
logger.info(f"Subject   : {report['email_subject']}")
logger.info(f"\nAll issues ({len(report['issues_detected'])}):")
for issue in report["issues_detected"]:
    logger.info(f"  [{issue['severity']}] {issue['check']}: {issue['message'][:120]}")

print("\n" + "=" * 60)
print("STRUCTURED JSON REPORT")
print("=" * 60)
print(json.dumps({
    "job_name":        report["job_name"],
    "status":          report["status"],
    "severity":        report["severity"],
    "issues_detected": len(report["issues_detected"]),
    "root_cause":      report["root_cause"][:300],
    "confidence":      report["confidence"],
    "email_subject":   report["email_subject"],
}, indent=2))

# ── Send email ────────────────────────────────────────────────────────────────

if should_alert(report):
    logger.info(f"\nSending alert to {os.getenv('ALERT_EMAIL')}...")
    sent = send_alert_email(
        report,
        gmail_user     = os.getenv("GMAIL_USER"),
        gmail_app_pass = os.getenv("GMAIL_APP_PASS"),
        to_email       = os.getenv("ALERT_EMAIL", "itsnishant56@gmail.com"),
    )
    if sent:
        logger.info("✅ Snowflake S3 integration alert sent successfully!")
    else:
        logger.error("❌ Email send failed — check GMAIL credentials in .env")
