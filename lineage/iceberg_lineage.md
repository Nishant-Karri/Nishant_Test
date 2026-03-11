# Data Lineage — `NISHANT_ICEBERG_DB.ICEBERG_SCHEMA.JSON_TO_PARQUET_ICEBERG`

> **Generated:** March 11, 2026
> **Source:** Snowflake Account Usage (`ACCOUNT_USAGE.OBJECT_DEPENDENCIES`, `ACCESS_HISTORY`, `TASK_HISTORY`)
> **Account:** `PHTIMLK-UZ24815` · **Role:** `ACCOUNTADMIN`

---

## Lineage Flow Diagram

```
┌─────────────────────────────────────────────────────────┐
│                     AWS S3 (Source)                     │
│         s3://nishant-test-nonprod-iceberg/              │
│                   Region: us-east-1                     │
└────────────────────────┬────────────────────────────────┘
                         │ Storage Integration
                         │ SNOWFLAKE_S3_INTEGRATION
                         ▼
┌─────────────────────────────────────────────────────────┐
│               ICEBERG_S3_STAGE (External Stage)         │
│   NISHANT_ICEBERG_DB.ICEBERG_SCHEMA.ICEBERG_S3_STAGE   │
│   Type: External · Cloud: AWS · Region: us-east-1      │
│   Owner: ACCOUNTADMIN                                   │
└────────────────────────┬────────────────────────────────┘
                         │ COPY INTO / External Table
                         ▼
┌─────────────────────────────────────────────────────────┐
│         JSON_TO_PARQUET_ICEBERG  ◄── SOURCE TABLE       │
│   NISHANT_ICEBERG_DB.ICEBERG_SCHEMA.JSON_TO_PARQUET_ICEBERG │
│   Type: Iceberg Table (BASE TABLE)                      │
│   Rows: 1,000 · Size: 23,416 bytes                      │
│   Created:  2026-03-10 14:23:57 PST                     │
│   Modified: 2026-03-10 14:24:01 PST                     │
│   Columns: ID (TEXT), NAME (TEXT)                       │
└────────────────────────┬────────────────────────────────┘
                         │ READ by SYSTEM user
                         │ via Snowflake Task (hourly)
                         ▼
┌─────────────────────────────────────────────────────────┐
│        DBT_ICEBERG_REFRESH_TASK (Snowflake Task)        │
│   NISHANT_ICEBERG_DB.DBT_SCHEMA.DBT_ICEBERG_REFRESH_TASK│
│   Schedule: CRON 0 * * * * UTC  (every hour, top of hr) │
│   State: started (ACTIVE)                               │
│   Warehouse: COMPUTE_WH                                 │
│   Definition: CALL run_dbt_iceberg_refresh()            │
│   Owner: ACCOUNTADMIN                                   │
└────────────────────────┬────────────────────────────────┘
                         │ CREATE OR REPLACE TABLE
                         ▼
┌─────────────────────────────────────────────────────────┐
│      JSON_TO_PARQUET_ICEBERG_DBT  ◄── dbt OUTPUT       │
│   NISHANT_ICEBERG_DB.DBT_SCHEMA.JSON_TO_PARQUET_ICEBERG_DBT │
│   Type: BASE TABLE (Iceberg via dbt)                    │
│   Rows: 1,000 · Size: 17,408 bytes                      │
│   Columns: ID (TEXT), NAME (TEXT)                       │
│   Refresh: Full replace on every task run               │
└─────────────────────────────────────────────────────────┘
```

---

## Asset Inventory

| Asset | Type | Database | Schema | Owner | Created |
|-------|------|----------|--------|-------|---------|
| `JSON_TO_PARQUET_ICEBERG` | Iceberg Table (Source) | NISHANT_ICEBERG_DB | ICEBERG_SCHEMA | ACCOUNTADMIN | 2026-03-10 |
| `ICEBERG_S3_STAGE` | External Stage | NISHANT_ICEBERG_DB | ICEBERG_SCHEMA | ACCOUNTADMIN | 2026-03-10 |
| `DBT_ICEBERG_REFRESH_TASK` | Snowflake Task | NISHANT_ICEBERG_DB | DBT_SCHEMA | ACCOUNTADMIN | 2026-03-10 |
| `JSON_TO_PARQUET_ICEBERG_DBT` | Iceberg Table (dbt Output) | NISHANT_ICEBERG_DB | DBT_SCHEMA | ACCOUNTADMIN | 2026-03-10 |
| `SNOWFLAKE_S3_INTEGRATION` | Storage Integration | — | — | — | — |

---

## Source Table: `JSON_TO_PARQUET_ICEBERG`

| Property | Value |
|----------|-------|
| **Fully Qualified Name** | `NISHANT_ICEBERG_DB.ICEBERG_SCHEMA.JSON_TO_PARQUET_ICEBERG` |
| **Table Type** | Iceberg (BASE TABLE) |
| **Row Count** | 1,000 |
| **Size** | 23,416 bytes (~23 KB) |
| **Created** | 2026-03-10 14:23:57 PST |
| **Last Altered** | 2026-03-10 14:24:01 PST |
| **Owner** | ACCOUNTADMIN |

### Schema

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `ID` | TEXT | YES | Unique record identifier |
| `NAME` | TEXT | YES | Record name |

---

## Storage Layer

| Property | Value |
|----------|-------|
| **Stage Name** | `NISHANT_ICEBERG_DB.ICEBERG_SCHEMA.ICEBERG_S3_STAGE` |
| **Stage Type** | External |
| **Cloud Provider** | AWS |
| **S3 Bucket** | `s3://nishant-test-nonprod-iceberg/` |
| **Region** | `us-east-1` |
| **Storage Integration** | `SNOWFLAKE_S3_INTEGRATION` |
| **Credentials** | Via Storage Integration (no embedded keys) |
| **Encryption** | Managed by integration |

---

## Transformation Layer: dbt Task

| Property | Value |
|----------|-------|
| **Task Name** | `DBT_ICEBERG_REFRESH_TASK` |
| **Schema** | `NISHANT_ICEBERG_DB.DBT_SCHEMA` |
| **Schedule** | `CRON 0 * * * * UTC` — every hour at :00 |
| **State** | `started` (Active) |
| **Warehouse** | `COMPUTE_WH` |
| **Procedure Called** | `run_dbt_iceberg_refresh()` |
| **Output Table** | `NISHANT_ICEBERG_DB.DBT_SCHEMA.JSON_TO_PARQUET_ICEBERG_DBT` |
| **Transformation Mode** | `CREATE OR REPLACE` (full refresh each run) |

### Task Execution History (Last 10 Runs)

| Scheduled Time (UTC) | Start Time | Completed | Status | Duration |
|----------------------|------------|-----------|--------|----------|
| 2026-03-11 15:00 | 15:00:01 | 15:00:14 | ✅ SUCCEEDED | ~13s |
| 2026-03-11 14:00 | 14:00:01 | 14:00:14 | ✅ SUCCEEDED | ~13s |
| 2026-03-11 13:00 | 13:00:01 | 13:00:16 | ✅ SUCCEEDED | ~15s |
| 2026-03-11 12:00 | 12:00:00 | 12:00:05 | ✅ SUCCEEDED | ~5s |
| 2026-03-11 11:00 | 11:00:00 | 11:00:06 | ✅ SUCCEEDED | ~6s |
| 2026-03-11 10:00 | 10:00:01 | 10:00:09 | ✅ SUCCEEDED | ~8s |
| 2026-03-11 09:00 | 09:00:01 | 09:00:12 | ✅ SUCCEEDED | ~11s |
| 2026-03-11 08:00 | 08:00:01 | 08:00:07 | ✅ SUCCEEDED | ~6s |
| 2026-03-11 07:00 | 07:00:01 | 07:00:10 | ✅ SUCCEEDED | ~9s |
| 2026-03-11 06:00 | 06:00:01 | 06:00:10 | ✅ SUCCEEDED | ~9s |

> All 10 recorded runs succeeded. Average duration: ~9.5 seconds.

---

## Object Dependencies

| Referencing Object | Type | Depends On | Type | Dependency |
|-------------------|------|-----------|------|-----------|
| `ICEBERG_SCHEMA.ICEBERG_S3_STAGE` | Stage | `SNOWFLAKE_S3_INTEGRATION` | Integration | BY_ID |
| `DBT_SCHEMA.JSON_TO_PARQUET_ICEBERG_DBT` | Table | `ICEBERG_SCHEMA.JSON_TO_PARQUET_ICEBERG` | Table | READ (via Task) |
| `MICRO_FUTURES.LATEST_SIGNALS` | View | `MICRO_FUTURES.FUTURES_FORECAST` | Table | BY_NAME |

---

## Access History (Last 30 Days — Key Events)

| Timestamp (PST) | User | Operation | Objects Read | Objects Written |
|-----------------|------|-----------|-------------|----------------|
| 2026-03-11 15:00 | SYSTEM | dbt Refresh | `ICEBERG_SCHEMA.JSON_TO_PARQUET_ICEBERG` | `DBT_SCHEMA.JSON_TO_PARQUET_ICEBERG_DBT` |
| 2026-03-11 14:00 | SYSTEM | dbt Refresh | `ICEBERG_SCHEMA.JSON_TO_PARQUET_ICEBERG` | `DBT_SCHEMA.JSON_TO_PARQUET_ICEBERG_DBT` |
| 2026-03-11 13:00 | SYSTEM | dbt Refresh | `ICEBERG_SCHEMA.JSON_TO_PARQUET_ICEBERG` | `DBT_SCHEMA.JSON_TO_PARQUET_ICEBERG_DBT` |
| 2026-03-11 13:27 | NISHANTKARRI | INSERT (×10) | — | `MICRO_FUTURES.MCL_USD_BARREL_FORECAST` |
| 2026-03-11 12:17 | NISHANTKARRI | SELECT | `MICRO_FUTURES.LATEST_SIGNALS` | — |
| 2026-03-11 12:17 | NISHANTKARRI | INSERT | — | `MICRO_FUTURES.FUTURES_FORECAST` |

---

## Downstream Impact

If `JSON_TO_PARQUET_ICEBERG` is unavailable or schema changes:

```
JSON_TO_PARQUET_ICEBERG  (source — ICEBERG_SCHEMA)
        │
        └──► JSON_TO_PARQUET_ICEBERG_DBT  (dbt output — DBT_SCHEMA)
                    │
                    └──► [Any downstream consumers of DBT_SCHEMA]
```

> **Note:** `MICRO_FUTURES.FUTURES_FORECAST` and `MCL_USD_BARREL_FORECAST` are
> independent tables — not downstream of this Iceberg table.

---

## Data Quality Notes

| Check | Result |
|-------|--------|
| Row count consistency | Source: 1,000 rows → dbt output: 1,000 rows ✅ |
| Task reliability | 10/10 successful runs (100% success rate) ✅ |
| Schema drift | None detected — columns stable (ID, NAME) ✅ |
| Storage integration | Active and credential-free (uses IAM) ✅ |
| Avg task duration | ~9.5 seconds (healthy) ✅ |

---

## How to Manage

```sql
-- Suspend the hourly refresh task
ALTER TASK NISHANT_ICEBERG_DB.DBT_SCHEMA.DBT_ICEBERG_REFRESH_TASK SUSPEND;

-- Resume the task
ALTER TASK NISHANT_ICEBERG_DB.DBT_SCHEMA.DBT_ICEBERG_REFRESH_TASK RESUME;

-- Check task status
SHOW TASKS IN DATABASE NISHANT_ICEBERG_DB;

-- View recent task runs
SELECT * FROM snowflake.account_usage.task_history
WHERE database_name = 'NISHANT_ICEBERG_DB'
ORDER BY scheduled_time DESC LIMIT 10;

-- Query source table
SELECT * FROM NISHANT_ICEBERG_DB.ICEBERG_SCHEMA.JSON_TO_PARQUET_ICEBERG LIMIT 100;

-- Query dbt output
SELECT * FROM NISHANT_ICEBERG_DB.DBT_SCHEMA.JSON_TO_PARQUET_ICEBERG_DBT LIMIT 100;
```

---

*Document auto-generated by Claude Code from live Snowflake metadata — `ACCOUNT_USAGE` views + `INFORMATION_SCHEMA`.*
