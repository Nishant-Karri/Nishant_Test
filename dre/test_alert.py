"""
DRE Alert Test — fires against the force-failed Glue job run.
Run: python test_alert.py
"""
import json
import logging
import os
import sys

import boto3
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
logger = logging.getLogger("DRE-TEST")

sys.path.insert(0, os.path.dirname(__file__))

from checks.job_health import classify_failure, check_runtime_sla
from alerting.email_alert import build_report, send_alert_email, should_alert

# ── Hardcoded test inputs ─────────────────────────────────────────────────────

JOB_NAME      = "nishant-test-nonprod-json-to-parquet"
RUN_ID        = "jr_8c07f203ce8664c609ea2402835902ddc9beaf1acb86154e285a7d9057ae91af"
ERROR_MESSAGE = (
    "An error occurred while calling o106.getDynamicFrame. "
    "com.amazon.ws.emr.hadoop.fs.shaded.com.amazonaws.services.s3.model.AmazonS3Exception: "
    "The specified bucket does not exist (Service: Amazon S3; Status Code: 404; "
    "Error Code: NoSuchBucket)"
)
DURATION_MIN  = 1.0

PIPELINE_CFG = {
    "name":              "nishant-test-nonprod-json-to-parquet",
    "glue_job_name":     JOB_NAME,
    "target_tables":     ["nishant_test_nonprod_db.json_to_parquet_iceberg", "nishant_test_nonprod_db.json_to_parquet"],
    "freshness": {
        "key_column":        "DATE",
        "max_delay_minutes": 60,
    },
    "row_count": {
        "deviation_threshold_pct": 20,
    },
    "sla_runtime_minutes": 30,
}

# ── Build job_result dict (same shape as run_job_health_check output) ─────────

failure_type, remediation = classify_failure(ERROR_MESSAGE, "")
runtime_sla   = check_runtime_sla(DURATION_MIN, PIPELINE_CFG["sla_runtime_minutes"])

job_result = {
    "job_name":        PIPELINE_CFG["name"],
    "glue_job":        JOB_NAME,
    "run_id":          RUN_ID,
    "status":          "FAILED",
    "started_at":      "2026-03-13 00:18:47 UTC",
    "completed_at":    "2026-03-13 00:19:35 UTC",
    "duration_minutes": DURATION_MIN,
    "runtime_sla":     runtime_sla,
    "failure_type":    failure_type,
    "error_message":   ERROR_MESSAGE,
    "remediation":     remediation,
    "logs_tail":       ERROR_MESSAGE,
}

logger.info(f"Failure type classified as: {failure_type}")
logger.info(f"Remediation: {remediation}")

# ── Build report (no Snowflake needed for this test) ─────────────────────────

report = build_report(
    pipeline_cfg    = PIPELINE_CFG,
    job_result      = job_result,
    quality_results = [],
    schema_results  = [],
    drift_results   = [],
)

logger.info(f"Severity  : {report['severity']}")
logger.info(f"Confidence: {report['confidence']}")
logger.info(f"Subject   : {report['email_subject']}")
logger.info(f"\nIssues detected ({len(report['issues_detected'])}):")
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
    "root_cause":      report["root_cause"][:200],
    "confidence":      report["confidence"],
    "email_subject":   report["email_subject"],
}, indent=2))

# ── Send alert email ──────────────────────────────────────────────────────────

if should_alert(report):
    gmail_user     = os.getenv("GMAIL_USER")
    gmail_app_pass = os.getenv("GMAIL_APP_PASS")
    to_email       = os.getenv("ALERT_EMAIL", "itsnishant56@gmail.com")

    if not gmail_app_pass or gmail_app_pass == "your_16_char_app_password":
        logger.warning("GMAIL_APP_PASS not set in .env — skipping email send.")
        logger.info("EMAIL SUBJECT: " + report["email_subject"])
        logger.info("EMAIL BODY PREVIEW:\n" + report["email_body"][:800])
    else:
        logger.info(f"Sending alert email to {to_email}...")
        sent = send_alert_email(report, gmail_user, gmail_app_pass, to_email)
        if sent:
            logger.info(f"✅ Alert email sent successfully to {to_email}")
        else:
            logger.error("❌ Email send failed — check GMAIL credentials in .env")
else:
    logger.info("No alert needed — all checks passed.")
