"""
DRE Live Monitoring Dashboard — Flask backend
Serves real-time pipeline health data from logs, state, snapshots, AWS, and Snowflake.

Run:
    python dashboard_server.py
Then open: http://localhost:8055
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timezone

import boto3
import yaml
from flask import Flask, jsonify, render_template_string

logging.basicConfig(level=logging.WARNING)
app = Flask(__name__)

BASE = os.path.dirname(__file__)
LOG_FILE   = "/tmp/dre_monitor_error.log"
STATE_FILE = os.path.join(BASE, "config", "monitor_state.json")
CONFIG_FILE = os.path.join(BASE, "config", "pipelines.yaml")
SNAPSHOT_DIR = os.path.join(BASE, "config", "schema_snapshots")


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_config():
    with open(CONFIG_FILE) as f:
        return yaml.safe_load(f)

def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}

def load_snapshot(table: str) -> dict:
    safe = table.replace(".", "_").replace("/", "_")
    path = os.path.join(SNAPSHOT_DIR, f"{safe}.json")
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {}

def tail_log(n=120):
    try:
        with open(LOG_FILE, "r") as f:
            lines = f.readlines()
        return lines[-n:]
    except Exception:
        return []

def parse_log_entries(lines):
    entries = []
    pattern = re.compile(
        r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(\w+)\s+\[([^\]]+)\]\s+(.*)"
    )
    for line in lines:
        m = pattern.match(line.strip())
        if m:
            ts, level, logger, msg = m.groups()
            if any(skip in logger for skip in ["snowflake", "botocore", "numexpr", "urllib"]):
                continue
            entries.append({"ts": ts, "level": level, "logger": logger, "msg": msg})
    return entries

def get_glue_status(job_name: str) -> dict:
    try:
        session = boto3.Session(region_name=os.getenv("AWS_REGION", "us-east-1"))
        glue = session.client("glue")
        resp = glue.get_job_runs(JobName=job_name, MaxResults=5)
        runs = resp.get("JobRuns", [])
        if not runs:
            return {"status": "NO_RUNS", "runs": []}
        result = []
        for r in runs:
            started = r.get("StartedOn")
            completed = r.get("CompletedOn")
            duration = None
            if started and completed:
                duration = round((completed - started).total_seconds() / 60, 1)
            result.append({
                "id": r.get("Id", "")[:20] + "...",
                "state": r.get("JobRunState", "UNKNOWN"),
                "started": started.strftime("%Y-%m-%d %H:%M") if started else "—",
                "duration_min": duration,
                "error": (r.get("ErrorMessage") or "")[:120],
            })
        return {"status": result[0]["state"], "runs": result}
    except Exception as e:
        return {"status": "ERROR", "error": str(e), "runs": []}

def get_snowflake_schema(table: str) -> dict:
    try:
        import snowflake.connector
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
        )
        db, sch, tbl = table.split(".")
        cur = conn.cursor()
        cur.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION
            FROM {db}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{sch}' AND TABLE_NAME = '{tbl}'
            ORDER BY ORDINAL_POSITION
        """)
        cols = [{"name": r[0], "type": r[1], "nullable": r[2]} for r in cur.fetchall()]
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        row_count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return {"columns": cols, "row_count": row_count, "error": None}
    except Exception as e:
        return {"columns": [], "row_count": None, "error": str(e)}

def compare_schema(current_cols, snapshot_cols):
    current = {c["name"] for c in current_cols}
    snap_names = set(snapshot_cols.keys())
    added   = list(current - snap_names)
    dropped = list(snap_names - current)
    changed = []
    for c in current_cols:
        if c["name"] in snapshot_cols:
            snap_type = snapshot_cols[c["name"]].get("type")
            if snap_type and snap_type != c["type"]:
                changed.append({"col": c["name"], "from": snap_type, "to": c["type"]})
    return {"added": added, "dropped": dropped, "changed": changed,
            "drift": bool(added or dropped or changed)}

def parse_alert_history(entries):
    alerts = []
    sent_pattern = re.compile(r"📧 Sent: (.+)")
    fail_pattern = re.compile(r"❌ Email failed: (.+)")
    supp_pattern = re.compile(r"⏳ Alert suppressed.*: (.+)")
    for e in reversed(entries):
        msg = e["msg"]
        for pat, status in [(sent_pattern, "sent"), (fail_pattern, "failed"), (supp_pattern, "suppressed")]:
            m = pat.search(msg)
            if m:
                alerts.append({"ts": e["ts"], "subject": m.group(1), "status": status})
                break
        if len(alerts) >= 20:
            break
    return alerts


# ── API ───────────────────────────────────────────────────────────────────────

@app.route("/api/status")
def api_status():
    cfg    = load_config()
    state  = load_state()
    settings = cfg.get("settings", {})
    jobs   = cfg.get("jobs", [])

    # Recipients
    emails = settings.get("alert_emails") or settings.get("alert_email", [])
    if isinstance(emails, str):
        emails = [emails]

    # Log entries
    raw_lines = tail_log(200)
    log_entries = parse_log_entries(raw_lines)
    alert_history = parse_alert_history(log_entries)

    # Monitor process alive?
    monitor_pid = None
    monitor_alive = False
    try:
        import subprocess
        result = subprocess.run(
            ["pgrep", "-f", "monitor.py"], capture_output=True, text=True
        )
        if result.returncode == 0:
            monitor_pid = result.stdout.strip().split("\n")[0]
            monitor_alive = True
    except Exception:
        pass

    # Per-job data
    jobs_data = []
    for job in jobs:
        jname = job.get("glue_job_name") or job.get("name")
        job_state = state.get(jname, {})

        # Glue runs
        glue = get_glue_status(jname)

        # Alert cooldowns
        cooldowns = {}
        sent_at = job_state.get("alert_sent_at", {})
        now = time.time()
        for cat, ts in sent_at.items():
            ago_min = int((now - ts) / 60)
            cooldowns[cat] = {"last_sent_min_ago": ago_min,
                              "on_cooldown": (now - ts) < 3600}

        # Tables
        tables_data = []
        for tbl in job.get("target_tables", []):
            tname = tbl["name"]
            snap  = load_snapshot(tname)
            sf    = get_snowflake_schema(tname)
            drift = compare_schema(sf["columns"], snap.get("columns", {})) if sf["columns"] else None
            tables_data.append({
                "name": tname,
                "row_count": sf.get("row_count"),
                "columns": sf.get("columns", []),
                "snapshot_cols": list(snap.get("columns", {}).keys()),
                "snapshot_captured": snap.get("captured_at"),
                "schema_drift": drift,
                "sf_error": sf.get("error"),
            })

        jobs_data.append({
            "name": jname,
            "sla_minutes": job.get("sla_runtime_minutes"),
            "freshness": job.get("freshness"),
            "glue": glue,
            "last_run_id": job_state.get("last_run_id", "—"),
            "cooldowns": cooldowns,
            "tables": tables_data,
        })

    return jsonify({
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "monitor": {"alive": monitor_alive, "pid": monitor_pid},
        "settings": settings,
        "recipients": emails,
        "jobs": jobs_data,
        "log_entries": log_entries[-60:],
        "alert_history": alert_history,
    })


# ── HTML dashboard ────────────────────────────────────────────────────────────

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>DRE Monitor — Live Dashboard</title>
<style>
  :root {
    --bg: #080c10; --surface: #0d1117; --border: #21262d;
    --text: #e6edf3; --muted: #7d8590; --accent: #58a6ff;
    --green: #3fb950; --yellow: #d29922; --orange: #f0883e;
    --red: #f85149; --purple: #bc8cff;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: -apple-system, 'Segoe UI', sans-serif; font-size: 14px; }

  /* Layout */
  .topbar { background: var(--surface); border-bottom: 1px solid var(--border);
    padding: 14px 28px; display: flex; align-items: center; justify-content: space-between; position: sticky; top: 0; z-index: 100; }
  .topbar h1 { font-size: 18px; font-weight: 700; letter-spacing: -0.3px; }
  .topbar h1 span { color: var(--accent); }
  .topbar-right { display: flex; align-items: center; gap: 18px; font-size: 12px; color: var(--muted); }
  .content { max-width: 1400px; margin: 0 auto; padding: 24px 28px; display: grid;
    grid-template-columns: 1fr 1fr 1fr; gap: 18px; }

  /* Cards */
  .card { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 20px; }
  .card.full { grid-column: 1 / -1; }
  .card.half { grid-column: span 2; }
  .card h2 { font-size: 12px; text-transform: uppercase; letter-spacing: 1px; color: var(--muted);
    margin-bottom: 14px; display: flex; align-items: center; gap: 8px; }
  .card h2 .dot { width: 7px; height: 7px; border-radius: 50%; background: var(--accent); }

  /* Badges */
  .badge { display: inline-flex; align-items: center; gap: 5px; padding: 3px 10px;
    border-radius: 20px; font-size: 11px; font-weight: 700; letter-spacing: 0.5px; }
  .badge-green  { background: #1a3a2a; color: var(--green);  border: 1px solid #2d5a3d; }
  .badge-red    { background: #3a1a1a; color: var(--red);    border: 1px solid #5a2d2d; }
  .badge-yellow { background: #3a2e1a; color: var(--yellow); border: 1px solid #5a481d; }
  .badge-orange { background: #3a2a1a; color: var(--orange); border: 1px solid #5a3d2d; }
  .badge-blue   { background: #1a2a3a; color: var(--accent); border: 1px solid #2d3d5a; }
  .badge-purple { background: #2a1a3a; color: var(--purple); border: 1px solid #3d2d5a; }
  .badge-muted  { background: #1a1e26; color: var(--muted);  border: 1px solid var(--border); }

  /* Stats row */
  .stats-row { grid-column: 1 / -1; display: grid; grid-template-columns: repeat(6, 1fr); gap: 14px; }
  .stat-card { background: var(--surface); border: 1px solid var(--border); border-radius: 10px;
    padding: 16px 18px; }
  .stat-card .label { font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 6px; }
  .stat-card .value { font-size: 26px; font-weight: 800; line-height: 1; }
  .stat-card .sub { font-size: 11px; color: var(--muted); margin-top: 4px; }

  /* Table */
  .data-table { width: 100%; border-collapse: collapse; font-size: 12px; }
  .data-table th { color: var(--muted); text-align: left; padding: 6px 12px; border-bottom: 1px solid var(--border);
    text-transform: uppercase; font-size: 10px; letter-spacing: 0.8px; font-weight: 600; }
  .data-table td { padding: 8px 12px; border-bottom: 1px solid #161b22; vertical-align: middle; }
  .data-table tr:last-child td { border-bottom: none; }
  .data-table tr:hover td { background: #161b22; }

  /* Monitor status hero */
  .status-hero { display: flex; align-items: center; gap: 16px; padding: 16px 0; }
  .pulse-ring { position: relative; width: 44px; height: 44px; }
  .pulse-ring .ring { position: absolute; inset: 0; border-radius: 50%; }
  .pulse-ring .inner { background: var(--green); }
  .pulse-ring .outer { border: 2px solid var(--green); animation: pulse 2s ease-out infinite; }
  .pulse-ring.dead .inner { background: var(--red); }
  .pulse-ring.dead .outer { border-color: var(--red); animation: none; }
  @keyframes pulse { 0%{transform:scale(1);opacity:1} 100%{transform:scale(1.8);opacity:0} }
  .status-info .name { font-size: 18px; font-weight: 800; }
  .status-info .sub  { color: var(--muted); font-size: 12px; margin-top: 2px; }

  /* Glue runs */
  .run-row { display: flex; align-items: center; gap: 10px; padding: 10px 0;
    border-bottom: 1px solid var(--border); }
  .run-row:last-child { border-bottom: none; }
  .run-state { min-width: 80px; }
  .run-meta { flex: 1; font-size: 12px; color: var(--muted); }
  .run-error { font-size: 11px; color: var(--red); margin-top: 3px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 340px; }

  /* Schema columns */
  .col-grid { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 6px; }
  .col-chip { padding: 4px 10px; border-radius: 6px; font-size: 11px; font-weight: 500;
    background: #161b22; border: 1px solid var(--border); color: var(--text); }
  .col-chip.added   { background: #1a3a2a; border-color: #3fb950; color: var(--green); }
  .col-chip.dropped { background: #3a1a1a; border-color: #f85149; color: var(--red); }
  .col-chip.changed { background: #3a2e1a; border-color: #d29922; color: var(--yellow); }

  /* Log feed */
  .log-feed { height: 340px; overflow-y: auto; font-family: 'SF Mono', 'Fira Code', monospace;
    font-size: 11px; line-height: 1.7; background: #050810; border-radius: 8px;
    padding: 12px 14px; border: 1px solid var(--border); }
  .log-feed::-webkit-scrollbar { width: 4px; }
  .log-feed::-webkit-scrollbar-thumb { background: var(--border); border-radius: 2px; }
  .log-line { display: flex; gap: 10px; }
  .log-ts   { color: #3a4a5a; white-space: nowrap; flex-shrink: 0; }
  .log-lvl  { width: 38px; flex-shrink: 0; font-weight: 700; }
  .log-logger { color: #4a5a7a; flex-shrink: 0; max-width: 160px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .log-msg  { color: #c0ccd8; word-break: break-word; }
  .lvl-INFO    { color: #4a8a5a; }
  .lvl-WARNING { color: var(--yellow); }
  .lvl-ERROR   { color: var(--red); }
  .lvl-DEBUG   { color: var(--muted); }

  /* Alert history */
  .alert-item { display: flex; align-items: flex-start; gap: 12px; padding: 10px 0;
    border-bottom: 1px solid var(--border); }
  .alert-item:last-child { border-bottom: none; }
  .alert-icon { font-size: 16px; margin-top: 1px; flex-shrink: 0; }
  .alert-subject { font-size: 12px; font-weight: 600; }
  .alert-meta { font-size: 11px; color: var(--muted); margin-top: 2px; }

  /* Cooldown */
  .cooldown-bar { height: 4px; background: var(--border); border-radius: 2px; margin-top: 6px; overflow: hidden; }
  .cooldown-fill { height: 100%; border-radius: 2px; transition: width 0.3s; }

  /* Recipients */
  .recipient { display: flex; align-items: center; gap: 8px; padding: 8px 0;
    border-bottom: 1px solid var(--border); }
  .recipient:last-child { border-bottom: none; }
  .recipient-avatar { width: 28px; height: 28px; border-radius: 50%;
    background: linear-gradient(135deg, var(--accent), var(--purple));
    display: flex; align-items: center; justify-content: font-size: 11px; font-weight: 700;
    justify-content: center; font-size: 11px; font-weight: 700; flex-shrink: 0; }
  .recipient-email { font-size: 12px; }

  /* Refresh countdown */
  .refresh-ring { position: relative; width: 32px; height: 32px; }
  .refresh-ring svg { transform: rotate(-90deg); }
  .refresh-ring circle { fill: none; stroke: var(--border); stroke-width: 2.5; }
  .refresh-ring .progress { stroke: var(--accent); stroke-linecap: round; transition: stroke-dashoffset 1s linear; }
  .refresh-text { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center;
    font-size: 9px; font-weight: 700; color: var(--accent); }

  /* Spinner */
  .spin { animation: spin 1s linear infinite; display: inline-block; }
  @keyframes spin { to { transform: rotate(360deg); } }

  /* Section divider */
  .section-label { grid-column: 1/-1; font-size: 11px; text-transform: uppercase; letter-spacing: 1.2px;
    color: var(--muted); padding-top: 8px; border-top: 1px solid var(--border); }

  /* Empty state */
  .empty { text-align: center; padding: 32px; color: var(--muted); font-size: 13px; }

  /* Error banner */
  .error-banner { grid-column: 1/-1; background: #3a1a1a; border: 1px solid #5a2d2d;
    border-radius: 10px; padding: 14px 18px; color: var(--red); font-size: 13px; }
</style>
</head>
<body>

<div class="topbar">
  <h1>⚡ DRE <span>Monitor</span></h1>
  <div class="topbar-right">
    <span id="last-updated">Loading…</span>
    <div class="refresh-ring" title="Auto-refresh every 30s">
      <svg width="32" height="32" viewBox="0 0 32 32">
        <circle cx="16" cy="16" r="13"/>
        <circle class="progress" id="ring-progress" cx="16" cy="16" r="13"
          stroke-dasharray="81.68" stroke-dashoffset="81.68"/>
      </svg>
      <div class="refresh-text" id="ring-count">30</div>
    </div>
  </div>
</div>

<div class="content" id="dashboard">
  <div class="empty" style="grid-column:1/-1;padding:80px">
    <div class="spin" style="font-size:32px">⟳</div>
    <p style="margin-top:16px">Loading dashboard…</p>
  </div>
</div>

<script>
const REFRESH = 30;
let countdown = REFRESH;
let timer;

function severity_color(s) {
  return s === 'CRITICAL' ? 'red' : s === 'HIGH' ? 'orange' : s === 'MEDIUM' ? 'yellow' : 'green';
}

function state_badge(s) {
  const map = {
    SUCCEEDED: 'green', RUNNING: 'blue', STARTING: 'blue', WAITING: 'blue',
    FAILED: 'red', TIMEOUT: 'orange', STOPPED: 'yellow', ERROR: 'red',
    NO_RUNS: 'muted', UNKNOWN: 'muted'
  };
  return `<span class="badge badge-${map[s] || 'muted'}">${s}</span>`;
}

function level_class(lvl) {
  return 'lvl-' + lvl;
}

function alert_icon(status, subject) {
  if (status === 'suppressed') return '⏳';
  if (status === 'failed') return '❌';
  if (subject.includes('SCHEMA')) return '🔍';
  if (subject.includes('JOB FAILURE')) return '🚨';
  if (subject.includes('FRESHNESS')) return '🕐';
  if (subject.includes('DATA DRIFT')) return '📊';
  return '📧';
}

function cooldown_html(cat, cd) {
  const pct = cd.on_cooldown ? Math.min(100, Math.round(cd.last_sent_min_ago / 60 * 100)) : 100;
  const color = cd.on_cooldown ? 'var(--yellow)' : 'var(--green)';
  const label = cd.on_cooldown ? `${60 - cd.last_sent_min_ago}min remaining` : `sent ${cd.last_sent_min_ago}min ago`;
  return `
    <div style="margin-bottom:8px">
      <div style="display:flex;justify-content:space-between;font-size:11px">
        <span style="color:var(--muted)">${cat}</span>
        <span style="color:${color}">${label}</span>
      </div>
      <div class="cooldown-bar">
        <div class="cooldown-fill" style="width:${pct}%;background:${color}"></div>
      </div>
    </div>`;
}

function render(data) {
  const { monitor, jobs, log_entries, alert_history, recipients, settings, ts } = data;

  // Counts
  const totalAlerts = alert_history.filter(a => a.status === 'sent').length;
  const schemaDrifts = alert_history.filter(a => a.status === 'sent' && a.subject.includes('SCHEMA')).length;
  const lastAlert = alert_history.find(a => a.status === 'sent');
  const jobCount = jobs.length;

  // Glue overall
  const allStates = jobs.map(j => j.glue?.status || 'UNKNOWN');
  const anyFailed = allStates.some(s => s === 'FAILED');
  const anyRunning = allStates.some(s => ['RUNNING','STARTING'].includes(s));

  let html = '';

  // ── Stats row ──────────────────────────────────────────────────────────────
  html += `<div class="stats-row">`;

  const monColor = monitor.alive ? 'var(--green)' : 'var(--red)';
  html += `<div class="stat-card">
    <div class="label">Monitor</div>
    <div class="value" style="color:${monColor};font-size:20px">${monitor.alive ? '● LIVE' : '● DOWN'}</div>
    <div class="sub">PID ${monitor.pid || '—'}</div>
  </div>`;

  html += `<div class="stat-card">
    <div class="label">Jobs Watched</div>
    <div class="value" style="color:var(--accent)">${jobCount}</div>
    <div class="sub">auto-discovered</div>
  </div>`;

  html += `<div class="stat-card">
    <div class="label">Pipeline Status</div>
    <div class="value" style="color:${anyFailed?'var(--red)':anyRunning?'var(--accent)':'var(--green)'}; font-size:18px">
      ${anyFailed ? '✖ FAILED' : anyRunning ? '⟳ RUNNING' : '✔ HEALTHY'}
    </div>
    <div class="sub">${allStates.join(', ')}</div>
  </div>`;

  html += `<div class="stat-card">
    <div class="label">Alerts Sent</div>
    <div class="value" style="color:var(--yellow)">${totalAlerts}</div>
    <div class="sub">last 120 log lines</div>
  </div>`;

  html += `<div class="stat-card">
    <div class="label">Schema Drifts</div>
    <div class="value" style="color:var(--purple)">${schemaDrifts}</div>
    <div class="sub">detected & alerted</div>
  </div>`;

  html += `<div class="stat-card">
    <div class="label">Last Alert</div>
    <div class="value" style="font-size:13px;margin-top:4px;font-weight:600">${lastAlert ? lastAlert.ts.split(' ')[1] : '—'}</div>
    <div class="sub">${lastAlert ? lastAlert.ts.split(' ')[0] : 'no alerts yet'}</div>
  </div>`;

  html += `</div>`;

  // ── Section: Jobs ──────────────────────────────────────────────────────────
  html += `<div class="section-label">Pipeline Jobs</div>`;

  for (const job of jobs) {
    const glue = job.glue;
    const runs = glue.runs || [];

    // Cooldowns card
    html += `<div class="card">
      <h2><span class="dot"></span>Monitor Daemon</h2>
      <div class="status-hero">
        <div class="pulse-ring ${monitor.alive ? '' : 'dead'}">
          <div class="ring outer"></div>
          <div class="ring inner" style="margin:8px"></div>
        </div>
        <div class="status-info">
          <div class="name">${monitor.alive ? 'Running' : 'Stopped'}</div>
          <div class="sub">PID ${monitor.pid || '—'} · poll 60s · drift 3600s</div>
        </div>
      </div>
      <div style="margin-top:12px">
        <div style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.8px;margin-bottom:8px">Alert Cooldowns</div>
        ${Object.keys(job.cooldowns).length === 0
          ? `<div style="color:var(--muted);font-size:12px">No cooldowns active</div>`
          : Object.entries(job.cooldowns).map(([cat, cd]) => cooldown_html(cat, cd)).join('')}
      </div>
    </div>`;

    // Glue status card
    html += `<div class="card">
      <h2><span class="dot" style="background:${anyFailed?'var(--red)':'var(--green)'}"></span>Glue Job</h2>
      <div style="margin-bottom:12px">
        <div style="font-size:13px;font-weight:700;margin-bottom:4px">${job.name}</div>
        <div style="display:flex;gap:8px;flex-wrap:wrap">
          ${state_badge(glue.status)}
          ${job.sla_minutes ? `<span class="badge badge-muted">SLA ${job.sla_minutes}m</span>` : ''}
          ${job.freshness ? `<span class="badge badge-blue">Freshness: ${job.freshness.key_column}</span>` : `<span class="badge badge-muted">No Freshness</span>`}
        </div>
      </div>
      <div>
        ${runs.slice(0,4).map(r => `
          <div class="run-row">
            <div class="run-state">${state_badge(r.state)}</div>
            <div class="run-meta">
              <div>${r.started}${r.duration_min ? ` · ${r.duration_min}m` : ''}</div>
              ${r.error ? `<div class="run-error">⚠ ${r.error}</div>` : ''}
            </div>
          </div>`).join('')}
        ${runs.length === 0 ? '<div class="empty" style="padding:16px">No runs found</div>' : ''}
      </div>
    </div>`;

    // Recipients card
    html += `<div class="card">
      <h2><span class="dot" style="background:var(--purple)"></span>Alert Recipients</h2>
      ${recipients.map(e => `
        <div class="recipient">
          <div class="recipient-avatar">${e[0].toUpperCase()}</div>
          <div class="recipient-email">${e}</div>
          <span class="badge badge-green" style="margin-left:auto">Active</span>
        </div>`).join('')}
      <div style="margin-top:14px;padding-top:12px;border-top:1px solid var(--border)">
        <div style="font-size:11px;color:var(--muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:.8px">Settings</div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;font-size:11px">
          <div style="color:var(--muted)">Poll interval</div><div>${settings.poll_interval_seconds || 60}s</div>
          <div style="color:var(--muted)">Freshness check</div><div>${settings.freshness_check_interval_seconds || 300}s</div>
          <div style="color:var(--muted)">Drift check</div><div>${settings.drift_check_interval_seconds || 3600}s</div>
        </div>
      </div>
    </div>`;

    // Tables section
    html += `<div class="section-label">Target Tables</div>`;

    for (const tbl of job.tables) {
      const drift = tbl.schema_drift;
      const hasDrift = drift?.drift;
      const driftColor = hasDrift ? 'var(--yellow)' : 'var(--green)';

      html += `<div class="card half">
        <h2><span class="dot" style="background:${hasDrift?'var(--yellow)':'var(--green)'}"></span>
          ${tbl.name.split('.').pop()}
          <span style="font-size:10px;color:var(--muted);margin-left:4px;text-transform:none;letter-spacing:0">${tbl.name}</span>
        </h2>
        ${tbl.sf_error ? `<div class="badge badge-red" style="margin-bottom:10px">Snowflake error: ${tbl.sf_error.substring(0,60)}</div>` : ''}
        <div style="display:flex;gap:12px;margin-bottom:14px;flex-wrap:wrap">
          <div>
            <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.8px">Rows</div>
            <div style="font-size:20px;font-weight:800;color:var(--accent)">${tbl.row_count !== null ? tbl.row_count.toLocaleString() : '—'}</div>
          </div>
          <div>
            <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.8px">Columns</div>
            <div style="font-size:20px;font-weight:800">${tbl.columns.length}</div>
          </div>
          <div>
            <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.8px">Schema Drift</div>
            <div style="font-size:14px;font-weight:700;color:${driftColor};margin-top:3px">
              ${hasDrift ? `⚠ Detected` : '✔ Clean'}
            </div>
          </div>
          <div>
            <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.8px">Snapshot</div>
            <div style="font-size:11px;color:var(--muted);margin-top:4px">${tbl.snapshot_captured ? tbl.snapshot_captured.replace('T',' ').substring(0,16) : '—'}</div>
          </div>
        </div>

        ${hasDrift ? `<div style="background:#1a1600;border:1px solid #3a3000;border-radius:8px;padding:12px;margin-bottom:12px">
          <div style="font-size:11px;font-weight:700;color:var(--yellow);margin-bottom:8px">⚠ Schema Changes Detected</div>
          ${drift.added.length ? `<div style="margin-bottom:4px"><span style="color:var(--green);font-size:11px">+ Added: </span><span style="font-size:11px">${drift.added.join(', ')}</span></div>` : ''}
          ${drift.dropped.length ? `<div style="margin-bottom:4px"><span style="color:var(--red);font-size:11px">− Dropped: </span><span style="font-size:11px">${drift.dropped.join(', ')}</span></div>` : ''}
          ${drift.changed.length ? drift.changed.map(c=>`<div style="margin-bottom:4px"><span style="color:var(--yellow);font-size:11px">~ Changed: </span><span style="font-size:11px">${c.col} (${c.from}→${c.to})</span></div>`).join('') : ''}
        </div>` : ''}

        <div style="font-size:11px;color:var(--muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:.8px">Current Columns</div>
        <div class="col-grid">
          ${tbl.columns.map(c => {
            const isAdded  = drift?.added?.includes(c.name);
            const isChanged= drift?.changed?.map(x=>x.col).includes(c.name);
            const cls = isAdded ? 'added' : isChanged ? 'changed' : '';
            return `<div class="col-chip ${cls}" title="${c.type} · ${c.nullable === 'YES' ? 'nullable' : 'not null'}">${c.name}</div>`;
          }).join('')}
          ${tbl.columns.length === 0 ? '<span style="color:var(--muted);font-size:12px">No columns</span>' : ''}
        </div>
      </div>`;

      // Schema snapshot comparison
      html += `<div class="card">
        <h2><span class="dot" style="background:var(--purple)"></span>Snapshot Baseline</h2>
        <div style="font-size:11px;color:var(--muted);margin-bottom:8px">
          Saved: ${tbl.snapshot_captured ? tbl.snapshot_captured.replace('T',' ').substring(0,19) : 'none'}
        </div>
        <div class="col-grid">
          ${tbl.snapshot_cols.map(col => {
            const isDropped = drift?.dropped?.includes(col);
            return `<div class="col-chip ${isDropped ? 'dropped' : ''}">${col}</div>`;
          }).join('')}
          ${tbl.snapshot_cols.length === 0 ? '<span style="color:var(--muted);font-size:12px">No snapshot</span>' : ''}
        </div>
        ${hasDrift ? `
          <div style="margin-top:14px;padding-top:12px;border-top:1px solid var(--border)">
            <div style="font-size:11px;color:var(--muted);margin-bottom:6px">Legend</div>
            <div style="display:flex;gap:8px;flex-wrap:wrap">
              <div class="col-chip added">+ Added</div>
              <div class="col-chip dropped">− Dropped</div>
              <div class="col-chip changed">~ Changed</div>
            </div>
          </div>` : ''}
      </div>`;
    }
  }

  // ── Alert history ──────────────────────────────────────────────────────────
  html += `<div class="section-label">Alert History</div>`;

  html += `<div class="card half">
    <h2><span class="dot" style="background:var(--orange)"></span>Recent Alerts</h2>
    ${alert_history.length === 0
      ? '<div class="empty">No alerts in recent logs</div>'
      : alert_history.slice(0,15).map(a => `
        <div class="alert-item">
          <div class="alert-icon">${alert_icon(a.status, a.subject)}</div>
          <div style="flex:1;min-width:0">
            <div class="alert-subject">${a.subject}</div>
            <div class="alert-meta">${a.ts} · ${a.status === 'sent' ? '<span style="color:var(--green)">✔ Sent</span>' : a.status === 'suppressed' ? '<span style="color:var(--yellow)">⏳ Suppressed</span>' : '<span style="color:var(--red)">✖ Failed</span>'}</div>
          </div>
        </div>`).join('')}
  </div>`;

  // ── Log feed ───────────────────────────────────────────────────────────────
  html += `<div class="card">
    <h2><span class="dot" style="background:var(--muted)"></span>Live Log Feed</h2>
    <div class="log-feed" id="log-feed">
      ${log_entries.map(e => `
        <div class="log-line">
          <span class="log-ts">${e.ts.split(' ')[1]}</span>
          <span class="log-lvl ${level_class(e.level)}">${e.level.substring(0,4)}</span>
          <span class="log-logger">${e.logger}</span>
          <span class="log-msg">${e.msg.replace(/</g,'&lt;')}</span>
        </div>`).join('')}
    </div>
  </div>`;

  document.getElementById('dashboard').innerHTML = html;
  document.getElementById('last-updated').textContent = 'Updated ' + ts;

  // Scroll log to bottom
  const lf = document.getElementById('log-feed');
  if (lf) lf.scrollTop = lf.scrollHeight;
}

function startCountdown() {
  clearInterval(timer);
  countdown = REFRESH;
  const circumference = 81.68;
  const ring = document.getElementById('ring-progress');
  const cnt  = document.getElementById('ring-count');

  timer = setInterval(() => {
    countdown--;
    if (cnt) cnt.textContent = countdown;
    if (ring) {
      const offset = circumference * (1 - (REFRESH - countdown) / REFRESH);
      ring.style.strokeDashoffset = offset;
    }
    if (countdown <= 0) {
      clearInterval(timer);
      fetchData();
    }
  }, 1000);
}

async function fetchData() {
  try {
    const res = await fetch('/api/status');
    const data = await res.json();
    render(data);
  } catch(e) {
    document.getElementById('last-updated').textContent = 'Error: ' + e.message;
  }
  startCountdown();
}

fetchData();
</script>
</body>
</html>"""

@app.route("/")
def index():
    return render_template_string(DASHBOARD_HTML)


if __name__ == "__main__":
    # Load env from launchd plist values (same as monitor.py uses)
    env_defaults = {
        "SNOWFLAKE_ACCOUNT": "PHTIMLK-UZ24815",
        "SNOWFLAKE_USER": "NishantKarri",
        "SNOWFLAKE_PASSWORD": "Nishantsai@1220!",
        "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
        "SNOWFLAKE_DATABASE": "NISHANT_ICEBERG_DB",
        "SNOWFLAKE_SCHEMA": "DBT_SCHEMA",
        "AWS_REGION": "us-east-1",
    }
    for k, v in env_defaults.items():
        os.environ.setdefault(k, v)

    print("🖥  DRE Dashboard → http://localhost:8055")
    app.run(host="0.0.0.0", port=8055, debug=False)
