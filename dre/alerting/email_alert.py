"""
Email Alert Generator — builds and sends structured DRE alerts.
"""
import json
import logging
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)


SEVERITY_COLOR = {
    "LOW":      "#22c55e",
    "MEDIUM":   "#f59e0b",
    "HIGH":     "#f97316",
    "CRITICAL": "#ef4444",
}

SEVERITY_EMOJI = {
    "LOW":      "🟢",
    "MEDIUM":   "🟡",
    "HIGH":     "🟠",
    "CRITICAL": "🔴",
}


def determine_severity(all_issues: list) -> str:
    """Return highest severity from all detected issues."""
    priority = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}
    if not all_issues:
        return "LOW"
    return max(
        (i.get("severity", "LOW") for i in all_issues),
        key=lambda s: priority.get(s, 0),
    )


def compute_confidence(job_result: dict, quality_issues: list, drift_issues: list) -> int:
    """
    Estimate confidence in root cause diagnosis.
    Higher confidence when: job FAILED + multiple corroborating signals.
    """
    score = 50
    if job_result.get("status") == "FAILED":
        score += 20
        if job_result.get("failure_type") and job_result["failure_type"] != "unknown":
            score += 15
    if quality_issues:
        score += min(len(quality_issues) * 3, 15)
    if drift_issues:
        score += min(len(drift_issues) * 2, 10)
    return min(score, 99)


def build_root_cause(job_result: dict, quality_issues: list, drift_issues: list, schema_issues: list) -> str:
    """Build human-readable root cause explanation."""
    parts = []

    status = job_result.get("status", "UNKNOWN")
    if status == "FAILED":
        ft = job_result.get("failure_type", "unknown")
        err = job_result.get("error_message", "")
        parts.append(f"Pipeline job FAILED with failure type: {ft.upper().replace('_', ' ')}.")
        if err:
            parts.append(f"Error: {err[:300]}")

    if schema_issues:
        cols = [i.get("column", "") for i in schema_issues]
        parts.append(f"Schema drift detected on columns: {', '.join(cols)}. "
                     "This may have caused downstream processing failures.")

    if drift_issues:
        cols = list({i.get("column", "") for i in drift_issues})
        parts.append(f"Statistical data drift on {len(drift_issues)} metric(s) "
                     f"(columns: {', '.join(cols[:5])}). "
                     "Upstream source data distribution may have changed.")

    freshness = [i for i in quality_issues if i.get("check") == "freshness"]
    if freshness:
        parts.append("Freshness SLA breached — pipeline likely did not complete on schedule.")

    nulls = [i for i in quality_issues if i.get("check") == "null_validation"]
    if nulls:
        parts.append(f"Unexpected nulls in {len(nulls)} critical column(s), "
                     "suggesting incomplete or corrupted source data.")

    dups = [i for i in quality_issues if i.get("check") == "duplicate_detection"]
    if dups:
        parts.append("Duplicate primary keys detected, likely caused by re-processing or upsert logic failure.")

    if not parts:
        return "No definitive root cause identified. Job completed but data quality deviations observed."

    return " ".join(parts)


def build_recommended_fix(job_result: dict, all_issues: list) -> str:
    """Aggregate all remediation steps into actionable guidance."""
    steps = []

    rem = job_result.get("remediation", [])
    if rem:
        steps.extend(rem)

    checks_seen = set()
    for issue in all_issues:
        check = issue.get("check", "")
        if check not in checks_seen:
            checks_seen.add(check)
            if check == "freshness":
                steps.append("Re-run the pipeline job manually and verify data lands within SLA.")
            elif check == "null_validation":
                steps.append("Investigate source data for nulls in critical columns before next load.")
            elif check == "duplicate_detection":
                steps.append("Review merge/upsert logic; deduplicate source before reload.")
            elif check == "schema_drift":
                steps.append("Review schema change, update downstream DDL and dbt models if intentional.")
            elif check == "data_drift":
                steps.append("Validate source system for unexpected distribution changes or upstream bugs.")
            elif check == "row_count_vs_yesterday":
                steps.append("Verify source row count against expected volume; check for missing partitions.")

    if not steps:
        steps.append("Monitor next pipeline run. No immediate action required.")

    return "\n".join(f"  {i+1}. {s}" for i, s in enumerate(steps))


def build_email_html(report: dict) -> str:
    """Build styled HTML email body."""
    sev = report["severity"]
    color = SEVERITY_COLOR.get(sev, "#666")
    emoji = SEVERITY_EMOJI.get(sev, "⚪")

    issues_html = ""
    for issue in report.get("issues_detected", []):
        sev_i = issue.get("severity", "LOW")
        c = SEVERITY_COLOR.get(sev_i, "#666")
        issues_html += f"""
        <tr>
          <td style="padding:8px 12px;border-bottom:1px solid #222;">
            <span style="background:{c};color:#fff;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:700;">{sev_i}</span>
          </td>
          <td style="padding:8px 12px;border-bottom:1px solid #222;color:#ccc;font-size:13px;">
            {issue.get("check","").replace("_"," ").title()}
          </td>
          <td style="padding:8px 12px;border-bottom:1px solid #222;color:#eee;font-size:13px;">
            {issue.get("message","")}
          </td>
        </tr>"""

    tables_html = "".join(
        f"<li style='color:#ccc;margin:4px 0;'>{t}</li>"
        for t in report.get("affected_tables", [])
    )

    fix_html = report.get("recommended_fix", "").replace("\n", "<br>")

    return f"""
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#0A0A0A;font-family:Inter,-apple-system,sans-serif;">
  <div style="max-width:700px;margin:0 auto;padding:32px 16px;">

    <!-- Header -->
    <div style="background:linear-gradient(135deg,{color}22,#111);border:1px solid {color}44;
                border-radius:16px;padding:28px 32px;margin-bottom:24px;">
      <div style="font-size:36px;margin-bottom:8px;">{emoji}</div>
      <h1 style="color:#fff;font-size:24px;font-weight:900;margin:0 0 6px;">
        DATA PIPELINE ALERT
      </h1>
      <p style="color:#aaa;margin:0;font-size:14px;">{report["job_name"]} · {report["execution_time"]}</p>
    </div>

    <!-- Status bar -->
    <div style="display:flex;gap:16px;margin-bottom:24px;flex-wrap:wrap;">
      <div style="flex:1;background:#111;border:1px solid #222;border-radius:12px;padding:16px 20px;">
        <p style="color:#666;font-size:11px;margin:0 0 4px;text-transform:uppercase;letter-spacing:1px;">Status</p>
        <p style="color:{'#ef4444' if report['status']=='FAILED' else '#22c55e'};font-size:18px;font-weight:900;margin:0;">
          {report["status"]}
        </p>
      </div>
      <div style="flex:1;background:#111;border:1px solid #222;border-radius:12px;padding:16px 20px;">
        <p style="color:#666;font-size:11px;margin:0 0 4px;text-transform:uppercase;letter-spacing:1px;">Severity</p>
        <p style="color:{color};font-size:18px;font-weight:900;margin:0;">{sev}</p>
      </div>
      <div style="flex:1;background:#111;border:1px solid #222;border-radius:12px;padding:16px 20px;">
        <p style="color:#666;font-size:11px;margin:0 0 4px;text-transform:uppercase;letter-spacing:1px;">Confidence</p>
        <p style="color:#fff;font-size:18px;font-weight:900;margin:0;">{report["confidence"]}</p>
      </div>
      <div style="flex:1;background:#111;border:1px solid #222;border-radius:12px;padding:16px 20px;">
        <p style="color:#666;font-size:11px;margin:0 0 4px;text-transform:uppercase;letter-spacing:1px;">Issues</p>
        <p style="color:#fff;font-size:18px;font-weight:900;margin:0;">{len(report.get("issues_detected", []))}</p>
      </div>
    </div>

    <!-- Summary -->
    <div style="background:#111;border:1px solid #222;border-radius:12px;padding:20px 24px;margin-bottom:20px;">
      <h2 style="color:#fff;font-size:15px;font-weight:800;margin:0 0 10px;">📋 Summary</h2>
      <p style="color:#ccc;font-size:14px;line-height:1.6;margin:0;">{report.get("summary","")}</p>
    </div>

    <!-- Detected Issues -->
    <div style="background:#111;border:1px solid #222;border-radius:12px;padding:20px 24px;margin-bottom:20px;">
      <h2 style="color:#fff;font-size:15px;font-weight:800;margin:0 0 16px;">🚨 Detected Issues</h2>
      <table style="width:100%;border-collapse:collapse;">
        <thead>
          <tr style="color:#555;font-size:11px;text-transform:uppercase;">
            <th style="text-align:left;padding:6px 12px;">Severity</th>
            <th style="text-align:left;padding:6px 12px;">Check</th>
            <th style="text-align:left;padding:6px 12px;">Detail</th>
          </tr>
        </thead>
        <tbody>{issues_html}</tbody>
      </table>
    </div>

    <!-- Root Cause -->
    <div style="background:#111;border:1px solid #222;border-radius:12px;padding:20px 24px;margin-bottom:20px;">
      <h2 style="color:#fff;font-size:15px;font-weight:800;margin:0 0 10px;">🔍 Root Cause</h2>
      <p style="color:#ccc;font-size:14px;line-height:1.6;margin:0;">{report.get("root_cause","")}</p>
    </div>

    <!-- Affected Tables -->
    <div style="background:#111;border:1px solid #222;border-radius:12px;padding:20px 24px;margin-bottom:20px;">
      <h2 style="color:#fff;font-size:15px;font-weight:800;margin:0 0 10px;">🗄️ Affected Tables</h2>
      <ul style="margin:0;padding-left:20px;">{tables_html}</ul>
    </div>

    <!-- Recommended Fix -->
    <div style="background:#111;border:1px solid {color}44;border-radius:12px;padding:20px 24px;margin-bottom:20px;">
      <h2 style="color:#fff;font-size:15px;font-weight:800;margin:0 0 10px;">🔧 Recommended Fix</h2>
      <p style="color:#ccc;font-size:14px;line-height:1.8;margin:0;">{fix_html}</p>
    </div>

    <!-- Footer -->
    <p style="color:#333;font-size:12px;text-align:center;margin-top:32px;">
      Auto-generated by Data Reliability Engineer Monitor · {report["execution_time"]}
    </p>
  </div>
</body>
</html>"""


def send_alert_email(report: dict, gmail_user: str, gmail_app_pass: str, to_email: str):
    """Send HTML alert email via Gmail SMTP."""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = report["email_subject"]
    msg["From"] = gmail_user
    msg["To"] = to_email

    msg.attach(MIMEText(report["email_body"], "plain"))
    msg.attach(MIMEText(build_email_html(report), "html"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(gmail_user, gmail_app_pass)
            server.sendmail(gmail_user, to_email, msg.as_string())
        logger.info(f"Alert email sent to {to_email}: {report['email_subject']}")
        return True
    except Exception as e:
        logger.error(f"Failed to send alert email: {e}")
        return False


def build_report(
    pipeline_cfg: dict,
    job_result: dict,
    quality_results: list,
    schema_results: list,
    drift_results: list,
) -> dict:
    """Assemble the full structured DRE report."""

    all_issues = []

    # Job failure
    if job_result.get("status") == "FAILED":
        all_issues.append({
            "check": "job_failure",
            "severity": "CRITICAL",
            "message": f"Job FAILED: {job_result.get('error_message', 'unknown error')[:200]}",
        })

    # Runtime SLA
    rt_sla = job_result.get("runtime_sla", {})
    if rt_sla.get("breached"):
        all_issues.append({
            "check": "runtime_sla",
            "severity": "MEDIUM",
            "message": rt_sla["message"],
        })

    # Quality issues
    for qr in quality_results:
        all_issues.extend(qr.get("all_issues", []))

    # Schema issues
    for sr in schema_results:
        all_issues.extend(sr.get("issues", []))

    # Drift issues
    for dr in drift_results:
        all_issues.extend(dr.get("issues", []))

    severity = determine_severity(all_issues)
    confidence = compute_confidence(job_result, all_issues, [i for i in all_issues if i.get("check") == "data_drift"])
    root_cause = build_root_cause(job_result, all_issues, [i for i in all_issues if i.get("check") == "data_drift"],
                                   [i for i in all_issues if i.get("check") == "schema_drift"])
    fix = build_recommended_fix(job_result, all_issues)

    job_name = pipeline_cfg["name"]
    exec_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    status = job_result.get("status", "UNKNOWN")
    affected = pipeline_cfg.get("target_tables", [])

    summary = (
        f"Pipeline '{job_name}' completed with status {status}. "
        f"{len(all_issues)} issue(s) detected across {len(affected)} table(s). "
        f"Severity: {severity}."
    )

    email_subject = f"[DATA PIPELINE ALERT] {job_name} — {severity}"

    email_body = f"""Pipeline: {job_name}
Execution Time: {exec_time}
Status: {status}
Severity: {severity}

Summary:
{summary}

Detected Issues:
""" + "\n".join(f"  • [{i.get('severity','?')}] {i.get('check','?')}: {i.get('message','')}" for i in all_issues) + f"""

Root Cause:
{root_cause}

Affected Tables:
""" + "\n".join(f"  • {t}" for t in affected) + f"""

Recommended Fix:
{fix}

Confidence Score: {confidence}%
"""

    report = {
        "job_name": job_name,
        "status": status,
        "severity": severity,
        "execution_time": exec_time,
        "issues_detected": all_issues,
        "affected_tables": affected,
        "root_cause": root_cause,
        "recommended_fix": fix,
        "confidence": f"{confidence}%",
        "summary": summary,
        "email_subject": email_subject,
        "email_body": email_body,
    }

    return report


def should_alert(report: dict) -> bool:
    """Only alert if job failed, data quality issue detected, or anomaly found."""
    if report["status"] == "FAILED":
        return True
    if report["issues_detected"]:
        return True
    return False
