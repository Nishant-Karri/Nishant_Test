# Data Reliability Engineer (DRE) — Automated Pipeline Monitor

Automated monitoring daemon that watches AWS Glue pipelines and Snowflake tables 24/7.
Sends email alerts to **itsnishant56@gmail.com** automatically — no manual triggers needed.

---

## What It Monitors

| Trigger | Frequency | Description |
|---------|-----------|-------------|
| Glue job completion | Every 60s poll | Detects SUCCEEDED / FAILED runs, runs full DRE analysis |
| Freshness breach | Every 5 min | Alerts if latest data timestamp exceeds SLA |
| Schema drift | Every 1 hour | Detects added, dropped, or type-changed columns |
| Data drift | Every 1 hour | Statistical drift on numeric + categorical distributions |

---

## Automatic Alerts

An email alert fires automatically when **any** of the following is detected:

| Condition | Severity | Detection Method |
|-----------|----------|-----------------|
| Job FAILED | CRITICAL | Glue job state + CloudWatch log classification |
| Data not fresh | HIGH / CRITICAL | MAX(DATE) vs SLA threshold in minutes |
| Schema drift — type change | CRITICAL | Column type comparison vs saved snapshot |
| Schema drift — column dropped | HIGH | Column presence comparison vs saved snapshot |
| Schema drift — column added | MEDIUM | Column presence comparison vs saved snapshot |
| Data drift — numeric | HIGH / CRITICAL | Z-score > 3σ vs 7-day rolling baseline |
| Data drift — categorical | MEDIUM / HIGH | Distribution shift > 15% vs 7-day baseline |
| Duplicate primary keys | HIGH | SQL GROUP BY / HAVING COUNT > 1 |
| Null violations | MEDIUM / CRITICAL | NULL count on not-null key columns |
| Row count anomaly | MEDIUM / HIGH | > 20% deviation vs yesterday and 7-day average |
| Runtime SLA breach | MEDIUM | Job duration vs configured SLA in minutes |

---

## Folder Structure

```
dre/
├── monitor.py                  ← Main daemon — run this
├── requirements.txt            ← Python dependencies
├── .env.example                ← Credential template
├── config/
│   ├── pipelines.yaml          ← Pipeline + table config
│   └── schema_snapshots/       ← Auto-created: baseline schema per table
├── checks/
│   ├── job_health.py           ← Glue job status + CloudWatch log analysis
│   ├── data_quality.py         ← Row count, freshness, nulls, duplicates
│   ├── schema_drift.py         ← Schema snapshot comparison
│   └── data_drift.py           ← Statistical drift (Z-score + distribution)
└── alerting/
    └── email_alert.py          ← Report builder + HTML email sender
```

---

## Setup

### 1. Install dependencies
```bash
cd dre
pip install -r requirements.txt
```

### 2. Configure credentials
```bash
cp .env.example .env
```

Fill in `.env`:
```
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=us-east-1

SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ANALYTICS
SNOWFLAKE_SCHEMA=RETAIL

GMAIL_USER=itsnishant56@gmail.com
GMAIL_APP_PASS=<16-char Gmail App Password>
ALERT_EMAIL=itsnishant56@gmail.com
```

### 3. Configure pipelines
Edit `config/pipelines.yaml` to add or modify:
- Glue job names
- Target Snowflake tables
- Freshness SLA (minutes)
- Row count deviation threshold (%)
- Runtime SLA (minutes)
- Not-null columns, primary keys, categorical allowed values

### 4. Run
```bash
python monitor.py
```

The daemon runs forever. On first run it saves schema snapshots as baselines.
From the second run onward, drift is detected against those baselines.

---

## Email Alert Format

**Subject:** `[DATA PIPELINE ALERT] <Job Name> — <Severity>`

**Body includes:**
- Pipeline name, execution time, status
- Summary of all detected issues
- Root cause analysis with confidence score
- Affected tables
- Step-by-step recommended fix

---

## Pipelines Configured

| Pipeline | Glue Job | Target Tables | Freshness SLA |
|----------|----------|---------------|---------------|
| DBT_ICEBERG_REFRESH_TASK | dbt_iceberg_refresh | FACT_SALES, DIM_DATE, DIM_STORE | 60 min |
| FACT_SALES_DAILY_LOAD | fact_sales_daily_load | FACT_SALES | 120 min |

To add a new pipeline, append an entry to `config/pipelines.yaml` and restart the daemon.

---

## Architecture

```
                    ┌─────────────────────────┐
                    │     monitor.py (daemon)  │
                    │  Runs continuously 24/7  │
                    └────────────┬────────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              │                  │                  │
     Every 60s poll       Every 5 min        Every 1 hour
              │                  │                  │
   ┌──────────▼──────┐  ┌────────▼───────┐  ┌──────▼──────────┐
   │  Job Health     │  │  Freshness     │  │  Schema Drift   │
   │  (Glue + CW)    │  │  Check         │  │  + Data Drift   │
   └──────────┬──────┘  └────────┬───────┘  └──────┬──────────┘
              │                  │                  │
              └──────────────────┼──────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   build_report()         │
                    │   should_alert()?        │
                    └────────────┬────────────┘
                                 │ if YES
                    ┌────────────▼────────────┐
                    │  send_alert_email()      │
                    │  → itsnishant56@gmail.com│
                    └─────────────────────────┘
```

---

*DRE Monitor v1.0 — Non-Prod*
