"""
Job Health Check — polls AWS Glue for job run status and classifies failures.
"""
import re
import boto3
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


FAILURE_PATTERNS = {
    "schema_mismatch": [
        r"schema mismatch", r"column.*not found", r"AnalysisException",
        r"cannot resolve column", r"incompatible types",
    ],
    "data_error": [
        r"data type mismatch", r"null value.*not-null constraint",
        r"constraint violation", r"invalid value", r"parse error",
    ],
    "infrastructure": [
        r"OutOfMemoryError", r"Connection refused", r"SIGKILL",
        r"executor lost", r"container exited", r"oom-kill",
    ],
    "upstream_dependency": [
        r"table.*does not exist", r"S3.*NoSuchKey", r"FileNotFound",
        r"upstream.*failed", r"dependency.*unavailable",
        r"NoSuchBucket", r"bucket does not exist", r"AmazonS3Exception", r"Status Code: 404",
    ],
    "timeout": [
        r"timeout", r"exceeded.*time limit", r"job.*timed out",
        r"max runtime", r"DurationExceededException",
    ],
}

REMEDIATION = {
    "schema_mismatch": [
        "Compare source schema against target DDL.",
        "Run schema drift check on upstream table.",
        "Update Glue crawler or dbt schema YAML if schema changed intentionally.",
    ],
    "data_error": [
        "Check upstream data for nulls or unexpected values.",
        "Review data validation rules in the job.",
        "Inspect rejected records in the job error output.",
    ],
    "infrastructure": [
        "Increase Glue DPU allocation.",
        "Check AWS service health dashboard.",
        "Review memory-intensive transformations and optimize.",
    ],
    "upstream_dependency": [
        "Verify upstream job completed successfully.",
        "Check S3 source paths and file existence.",
        "Review pipeline DAG dependencies.",
    ],
    "timeout": [
        "Increase job timeout limit in Glue settings.",
        "Optimize slow transformations or add partitioning.",
        "Check for data volume spikes causing longer runs.",
    ],
}


def get_glue_job_status(glue_client, job_name: str, run_id: str = None) -> dict:
    """
    Fetch latest (or specific) Glue job run status.
    Returns normalized job run dict.
    """
    try:
        if run_id:
            resp = glue_client.get_job_run(JobName=job_name, RunId=run_id)
            run = resp["JobRun"]
        else:
            resp = glue_client.get_job_runs(JobName=job_name, MaxResults=1)
            runs = resp.get("JobRuns", [])
            if not runs:
                return {"error": f"No runs found for job {job_name}"}
            run = runs[0]

        started = run.get("StartedOn")
        completed = run.get("CompletedOn")
        duration_min = None
        if started and completed:
            duration_min = round((completed - started).total_seconds() / 60, 2)

        return {
            "job_name": job_name,
            "run_id": run.get("Id"),
            "status": run.get("JobRunState"),          # SUCCEEDED / FAILED / RUNNING / TIMEOUT
            "started_at": str(started),
            "completed_at": str(completed),
            "duration_minutes": duration_min,
            "error_message": run.get("ErrorMessage", ""),
            "attempt": run.get("Attempt", 0),
        }
    except Exception as e:
        logger.error(f"Failed to fetch Glue job status for {job_name}: {e}")
        return {"error": str(e)}


def get_cloudwatch_logs(logs_client, job_name: str, run_id: str, tail_lines: int = 100) -> str:
    """
    Fetch recent CloudWatch log events for a Glue job run.
    """
    log_group = f"/aws-glue/jobs/output"
    log_stream = run_id
    try:
        resp = logs_client.get_log_events(
            logGroupName=log_group,
            logStreamName=log_stream,
            limit=tail_lines,
            startFromHead=False,
        )
        events = resp.get("events", [])
        return "\n".join(e["message"] for e in events)
    except Exception as e:
        logger.warning(f"Could not fetch CloudWatch logs for {job_name}/{run_id}: {e}")
        return ""


def classify_failure(error_message: str, logs: str) -> tuple:
    """
    Classify failure type from error message and logs.
    Returns (failure_type, remediation_steps).
    """
    combined = f"{error_message}\n{logs}".lower()

    for failure_type, patterns in FAILURE_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, combined, re.IGNORECASE):
                return failure_type, REMEDIATION[failure_type]

    return "unknown", ["Review full CloudWatch logs for the job run.", "Check upstream dependencies manually."]


def check_runtime_sla(duration_minutes: float, sla_minutes: int) -> dict:
    """
    Check if job runtime exceeded SLA.
    """
    if duration_minutes is None:
        return {"breached": False, "message": "Duration not available"}

    breached = duration_minutes > sla_minutes
    return {
        "breached": breached,
        "duration_minutes": duration_minutes,
        "sla_minutes": sla_minutes,
        "overage_minutes": round(duration_minutes - sla_minutes, 2) if breached else 0,
        "message": (
            f"Runtime {duration_minutes}m exceeded SLA of {sla_minutes}m by {round(duration_minutes - sla_minutes, 2)}m"
            if breached
            else f"Runtime {duration_minutes}m within SLA of {sla_minutes}m"
        ),
    }


def run_job_health_check(glue_client, logs_client, pipeline_config: dict) -> dict:
    """
    Full job health check for a pipeline.
    Returns structured health result.
    """
    job_name = pipeline_config["glue_job_name"]
    sla = pipeline_config.get("sla_runtime_minutes", 60)

    run_info = get_glue_job_status(glue_client, job_name)
    if "error" in run_info:
        return {"status": "ERROR", "message": run_info["error"]}

    status = run_info["status"]
    logs = ""
    failure_type = None
    remediation = []

    if status == "FAILED":
        logs = get_cloudwatch_logs(logs_client, job_name, run_info["run_id"])
        failure_type, remediation = classify_failure(run_info["error_message"], logs)

    runtime_sla = check_runtime_sla(run_info["duration_minutes"], sla)

    return {
        "job_name": pipeline_config["name"],
        "glue_job": job_name,
        "run_id": run_info["run_id"],
        "status": status,
        "started_at": run_info["started_at"],
        "completed_at": run_info["completed_at"],
        "duration_minutes": run_info["duration_minutes"],
        "runtime_sla": runtime_sla,
        "failure_type": failure_type,
        "error_message": run_info["error_message"],
        "remediation": remediation,
        "logs_tail": logs[-2000:] if logs else "",
    }
