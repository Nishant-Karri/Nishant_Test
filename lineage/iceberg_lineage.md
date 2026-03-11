# Data Lineage — `NISHANT_ICEBERG_DB.ICEBERG_SCHEMA.JSON_TO_PARQUET_ICEBERG`

> **Generated:** March 11, 2026
> **Sources:** AWS Glue API · S3 API · Snowflake `ACCOUNT_USAGE` + `INFORMATION_SCHEMA`
> **AWS Account:** `717728193460` · **Snowflake Account:** `PHTIMLK-UZ24815`

---

## End-to-End Lineage Flow

```
┌──────────────────────────────────────────────────────────────────┐
│                        RAW SOURCE (S3)                           │
│   s3://nishant-test-nonprod-source-raw/raw/                      │
│   Format: JSON  (multiLine)  ·  Schema: { id, name }            │
└─────────────────────────┬────────────────────────────────────────┘
                          │ Input
                          ▼
┌──────────────────────────────────────────────────────────────────┐
│              AWS GLUE JOB  (Creation & Update Engine)            │
│   Name:    nishant-test-nonprod-json-to-parquet                  │
│   Script:  glue_json_to_parquet_v4.py (S3 / GitHub)             │
│   Runtime: Glue 4.0 · PySpark · Python 3                        │
│   Workers: 2 × G.1X · Max concurrency: 1 · Retries: 1          │
│   Role:    nishant-test-non-prod-glue-role (IAM)                 │
│   Trigger: GitHub Actions (GitHubActionsRole / CI-CD)           │
│                                                                  │
│   Steps:                                                         │
│   1. Configure Spark for Iceberg + Glue Catalog                  │
│   2. Read JSON from S3 (multiLine, recurse)                     │
│   3. Select columns: id, name                                    │
│   4. Write Parquet → target S3 (Snappy)                         │
│   5. Append to Iceberg → glue_catalog via .writeTo().append()   │
│   6. MSCK REPAIR TABLE on Athena parquet table                  │
└────┬────────────────────────────┬────────────────────────────────┘
     │ Parquet write              │ Iceberg write (.append())
     ▼                            ▼
┌──────────────────┐   ┌──────────────────────────────────────────┐
│  PARQUET TABLE   │   │         AWS S3 — Iceberg Storage         │
│  (Athena/Glue)   │   │  s3://nishant-test-nonprod-iceberg/      │
│                  │   │  json_to_parquet_iceberg/                │
│  DB: nishant_    │   │  ├── data/  (20 × .parquet, ~23KB total) │
│  test_nonprod_db │   │  └── metadata/                           │
│  Table:          │   │      ├── 00000-*.metadata.json  (v0)     │
│  json_to_parquet │   │      ├── 00001-*.metadata.json  (v1)     │
│  Format: Parquet │   │      ├── *-m0.avro  (manifest)           │
│  Compress: SNAPPY│   │      └── snap-9076158952705182747-*.avro  │
│  Cols: id, name  │   └──────────────────┬───────────────────────┘
└──────────────────┘                      │ Registered in Glue Catalog
                                          ▼
                       ┌──────────────────────────────────────────┐
                       │       AWS GLUE CATALOG (Iceberg)         │
                       │   DB:    nishant_test_nonprod_db         │
                       │   Table: json_to_parquet_iceberg         │
                       │   Type:  ICEBERG / EXTERNAL_TABLE        │
                       │   Cols:  id (string, field_id=1)         │
                       │          name (string, field_id=2)       │
                       │   Metadata: 00001-c84ce93c-*.json (v1)   │
                       └──────────────────┬───────────────────────┘
                                          │ Via SNOWFLAKE_S3_INTEGRATION
                                          ▼
┌──────────────────────────────────────────────────────────────────┐
│              SNOWFLAKE EXTERNAL STAGE                            │
│   NISHANT_ICEBERG_DB.ICEBERG_SCHEMA.ICEBERG_S3_STAGE            │
│   URL:  s3://nishant-test-nonprod-iceberg/  ·  us-east-1        │
│   Auth: SNOWFLAKE_S3_INTEGRATION (IAM, no embedded creds)       │
└─────────────────────────┬────────────────────────────────────────┘
                          │ Iceberg table registration
                          ▼
┌──────────────────────────────────────────────────────────────────┐
│           SNOWFLAKE ICEBERG TABLE  ◄── PRIMARY ASSET            │
│   NISHANT_ICEBERG_DB.ICEBERG_SCHEMA.JSON_TO_PARQUET_ICEBERG     │
│   Type: Iceberg (BASE TABLE)  ·  Rows: 1,000  ·  23,416 bytes  │
│   Created: 2026-03-10 14:23:57 PST                              │
│   Columns: ID (TEXT), NAME (TEXT)                               │
└─────────────────────────┬────────────────────────────────────────┘
                          │ READ hourly by Snowflake Task
                          ▼
┌──────────────────────────────────────────────────────────────────┐
│         SNOWFLAKE TASK — DBT_ICEBERG_REFRESH_TASK               │
│   NISHANT_ICEBERG_DB.DBT_SCHEMA                                  │
│   Schedule: CRON 0 * * * * UTC  (every hour at :00)            │
│   State: started (ACTIVE)  ·  Warehouse: COMPUTE_WH             │
│   Calls: CALL run_dbt_iceberg_refresh()                         │
└─────────────────────────┬────────────────────────────────────────┘
                          │ CREATE OR REPLACE (full refresh ~9.5s)
                          ▼
┌──────────────────────────────────────────────────────────────────┐
│           DBT OUTPUT TABLE                                       │
│   NISHANT_ICEBERG_DB.DBT_SCHEMA.JSON_TO_PARQUET_ICEBERG_DBT     │
│   Rows: 1,000  ·  Size: 17,408 bytes  ·  Cols: ID, NAME        │
└──────────────────────────────────────────────────────────────────┘
```

---

## Layer 1 — AWS Glue Job (How the Iceberg Table is Created & Updated)

### Job Details

| Property | Value |
|----------|-------|
| **Job Name** | `nishant-test-nonprod-json-to-parquet` |
| **Script** | `s3://nishant-test-nonprod-glue-scripts/scripts/glue_json_to_parquet_v4.py` |
| **Runtime** | Glue 4.0 · PySpark · Python 3 |
| **Worker Type** | G.1X |
| **Workers** | 2 |
| **Max Concurrency** | 1 |
| **Max Retries** | 1 |
| **Timeout** | 60 minutes |
| **IAM Role** | `arn:aws:iam::717728193460:role/nishant-test-non-prod-glue-role` |
| **Triggered By** | `GitHubActionsRole` (CI/CD) |
| **Bookmarking** | Enabled (`job-bookmark-enable`) |
| **Iceberg Extension** | `org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions` |

### Job Arguments

| Parameter | Value |
|-----------|-------|
| `--source_s3_path` | `s3://nishant-test-nonprod-source-raw/raw/` |
| `--target_s3_path` | `s3://nishant-test-nonprod-target-parquet/json_to_parquet/` |
| `--iceberg_s3_path` | `s3://nishant-test-nonprod-iceberg/json_to_parquet_iceberg/` |
| `--iceberg_warehouse` | `s3://nishant-test-nonprod-iceberg/warehouse/` |
| `--iceberg_table` | `json_to_parquet_iceberg` |
| `--athena_database` | `nishant_test_nonprod_db` |
| `--athena_table` | `json_to_parquet` |
| `--athena_output_s3` | `s3://nishant-test-nonprod-athena-results/results/` |
| `--datalake-formats` | `iceberg` |

### How the Job Creates the Iceberg Table

```python
# 1 — Configure Spark for Iceberg + AWS Glue Catalog
spark.conf.set("spark.sql.catalog.glue_catalog",
               "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse",
               "s3://nishant-test-nonprod-iceberg/warehouse/")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl",
               "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl",
               "org.apache.iceberg.aws.s3.S3FileIO")

# 2 — Read JSON from S3
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://nishant-test-nonprod-source-raw/raw/"],
                        "recurse": True},
    format="json",
    format_options={"multiLine": "true"},
)

# 3 — Select id and name only
df_selected = df.select(col("id"), col("name"))

# 4 — Write Parquet (Snappy) to Athena target
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame_selected, connection_type="s3",
    connection_options={"path": "s3://.../json_to_parquet/"},
    format="parquet",
    format_options={"compression": "snappy", "useGlueParquetWriter": "true"},
)

# 5 — Append to Iceberg table (creates if not exists, appends if exists)
df_selected.writeTo(
    "glue_catalog.nishant_test_nonprod_db.json_to_parquet_iceberg"
).append()

# 6 — Refresh Athena partition metadata
athena_client.start_query_execution(
    QueryString="MSCK REPAIR TABLE `nishant_test_nonprod_db`.`json_to_parquet`", ...
)
```

### Glue Job Run History (Last 5 Runs)

| Started (UTC) | Status | Duration | Notes |
|---------------|--------|----------|-------|
| 2026-03-09 19:42 | ✅ SUCCEEDED | 108s | v4 script — clean run |
| 2026-03-09 19:33 | ✅ SUCCEEDED | 78s | v4 script — clean run |
| 2026-03-09 19:31 | ❌ FAILED (retry) | 80s | `NameError: DynamicFrame not defined` (v3) |
| 2026-03-09 19:29 | ❌ FAILED | 85s | `NameError: DynamicFrame not defined` (v3) |
| 2026-03-09 16:14 | ❌ FAILED (retry) | 78s | `NameError: DynamicFrame not defined` (v3) |

> **Root cause of failures:** `glue_json_to_parquet_v3.py` was missing `from awsglue.dynamicframe import DynamicFrame`. Fixed in `v4`.

---

## Layer 2 — AWS S3 Storage

### S3 Buckets

| Bucket | Purpose |
|--------|---------|
| `nishant-test-nonprod-source-raw` | Raw JSON input (`/raw/`) |
| `nishant-test-nonprod-iceberg` | Iceberg table data + metadata |
| `nishant-test-nonprod-target-parquet` | Parquet output for Athena |
| `nishant-test-nonprod-glue-scripts` | Job scripts, temp files, Spark logs |
| `nishant-test-nonprod-athena-results` | Athena query results |

### Iceberg Table S3 Layout

```
s3://nishant-test-nonprod-iceberg/json_to_parquet_iceberg/
├── data/                          (20 Parquet files, ~23KB total)
│   ├── 00000-86-31d5a4a6-*.parquet
│   ├── 00001-87-4ed8eb47-*.parquet
│   └── ... (18 more files, written 2026-03-09 19:43)
└── metadata/
    ├── 00000-0a0421dc-*.metadata.json   (v0 — initial empty table, 767B)
    ├── 00001-c84ce93c-*.metadata.json   (v1 — after data load, 2,137B)
    ├── f1a16133-*-m0.avro               (manifest file, 7,608B)
    └── snap-9076158952705182747-*.avro  (snapshot file, 4,276B)
```

**Snapshot ID:** `9076158952705182747`
**Current metadata version:** v1

---

## Layer 3 — AWS Glue Data Catalog

| Property | Value |
|----------|-------|
| **Catalog ID** | `717728193460` |
| **Database** | `nishant_test_nonprod_db` |
| **Created By** | `GitHubActionsRole` |

| Table | Type | Format | Columns |
|-------|------|--------|---------|
| `json_to_parquet` | EXTERNAL_TABLE | Parquet/SNAPPY | id (string), name (string) |
| `json_to_parquet_iceberg` | ICEBERG EXTERNAL_TABLE | Iceberg | id (string, field_id=1), name (string, field_id=2) |

**Iceberg metadata pointer:** `s3://.../metadata/00001-c84ce93c-4e91-4261-88c4-5a9d926e5034.metadata.json`

---

## Layer 4 — Snowflake Integration

| Property | Value |
|----------|-------|
| **Storage Integration** | `SNOWFLAKE_S3_INTEGRATION` |
| **Stage** | `NISHANT_ICEBERG_DB.ICEBERG_SCHEMA.ICEBERG_S3_STAGE` |
| **S3 Bucket** | `s3://nishant-test-nonprod-iceberg/` · `us-east-1` |
| **Auth** | IAM role-based (no embedded credentials) |
| **Snowflake Table** | `NISHANT_ICEBERG_DB.ICEBERG_SCHEMA.JSON_TO_PARQUET_ICEBERG` |
| **Rows** | 1,000 · Size: 23,416 bytes |
| **Columns** | `ID TEXT`, `NAME TEXT` |

---

## Layer 5 — Snowflake dbt Refresh (Downstream)

| Property | Value |
|----------|-------|
| **Task** | `NISHANT_ICEBERG_DB.DBT_SCHEMA.DBT_ICEBERG_REFRESH_TASK` |
| **Schedule** | `CRON 0 * * * * UTC` — every hour |
| **State** | `started` (Active) |
| **Output** | `NISHANT_ICEBERG_DB.DBT_SCHEMA.JSON_TO_PARQUET_ICEBERG_DBT` |
| **Mode** | `CREATE OR REPLACE` (full refresh) |
| **Avg Duration** | ~9.5 seconds · Success rate: 10/10 (100%) |

---

## Full Asset Inventory

| # | Asset | Layer | Platform | Type |
|---|-------|-------|----------|------|
| 1 | `s3://nishant-test-nonprod-source-raw/raw/` | Source | AWS S3 | Raw JSON |
| 2 | `nishant-test-nonprod-json-to-parquet` (Glue Job) | Ingestion | AWS Glue | ETL Job |
| 3 | `glue_json_to_parquet_v4.py` | Ingestion | GitHub / S3 | Script |
| 4 | `s3://nishant-test-nonprod-iceberg/json_to_parquet_iceberg/` | Storage | AWS S3 | Iceberg files |
| 5 | `nishant_test_nonprod_db.json_to_parquet_iceberg` | Catalog | AWS Glue | Iceberg Table |
| 6 | `nishant_test_nonprod_db.json_to_parquet` | Catalog | AWS Glue / Athena | Parquet Table |
| 7 | `SNOWFLAKE_S3_INTEGRATION` | Integration | Snowflake / AWS | Storage Integration |
| 8 | `ICEBERG_SCHEMA.ICEBERG_S3_STAGE` | Integration | Snowflake | External Stage |
| 9 | `ICEBERG_SCHEMA.JSON_TO_PARQUET_ICEBERG` | Consumption | Snowflake | Iceberg Table |
| 10 | `DBT_SCHEMA.DBT_ICEBERG_REFRESH_TASK` | Transform | Snowflake | Scheduled Task |
| 11 | `DBT_SCHEMA.JSON_TO_PARQUET_ICEBERG_DBT` | Transform | Snowflake | dbt Output Table |

---

## Data Quality & Reliability

| Check | Result |
|-------|--------|
| Row consistency (S3 → Snowflake) | 1,000 rows ✅ |
| Glue job recent success rate | 2/5 (3 failed on v3 script — fixed in v4) ✅ |
| Snowflake task success rate | 10/10 (100%) ✅ |
| Schema drift | None — columns stable (id, name) ✅ |
| Iceberg metadata versions | v0 (empty) → v1 (with data) ✅ |
| Storage auth | IAM role-based (no embedded keys) ✅ |

---

## Management Commands

```bash
# Trigger Glue job manually
aws glue start-job-run \
  --job-name nishant-test-nonprod-json-to-parquet --region us-east-1

# Check last 5 job runs
aws glue get-job-runs \
  --job-name nishant-test-nonprod-json-to-parquet \
  --region us-east-1 --max-results 5

# List Iceberg files in S3
aws s3 ls s3://nishant-test-nonprod-iceberg/json_to_parquet_iceberg/ --recursive
```

```sql
-- Query Snowflake Iceberg table
SELECT * FROM NISHANT_ICEBERG_DB.ICEBERG_SCHEMA.JSON_TO_PARQUET_ICEBERG LIMIT 100;

-- Suspend / resume hourly dbt refresh
ALTER TASK NISHANT_ICEBERG_DB.DBT_SCHEMA.DBT_ICEBERG_REFRESH_TASK SUSPEND;
ALTER TASK NISHANT_ICEBERG_DB.DBT_SCHEMA.DBT_ICEBERG_REFRESH_TASK RESUME;

-- View task run history
SELECT * FROM snowflake.account_usage.task_history
WHERE database_name = 'NISHANT_ICEBERG_DB'
ORDER BY scheduled_time DESC LIMIT 10;
```

---

*Auto-generated from live AWS Glue API, S3 API, and Snowflake account usage metadata.*
*AWS: `717728193460` · Snowflake: `PHTIMLK-UZ24815` · Branch: `non-prod`*

---

## Field Descriptions

### `ICEBERG_SCHEMA.JSON_TO_PARQUET_ICEBERG` (Snowflake — Source Iceberg Table)

| Column | Data Type | Nullable | Max Length | Iceberg Field ID | Description |
|--------|-----------|----------|------------|-----------------|-------------|
| `ID` | TEXT | YES | 134,217,728 | 1 | Unique string identifier for each record. Sourced directly from the raw JSON `id` field. Acts as the logical primary key. No deduplication enforced at ingestion — duplicates possible if source JSON contains repeated IDs. |
| `NAME` | TEXT | YES | 134,217,728 | 2 | Human-readable name or label associated with the record. Sourced from the raw JSON `name` field. Free-text; no normalisation or validation applied during Glue ETL. |

> **Max length note:** The 134M character limit on the Snowflake Iceberg table reflects the native Iceberg `string` type (unbounded). The Glue/Athena side enforces no length constraint either.

---

### `DBT_SCHEMA.JSON_TO_PARQUET_ICEBERG_DBT` (Snowflake — dbt Output Table)

| Column | Data Type | Nullable | Max Length | Description |
|--------|-----------|----------|------------|-------------|
| `ID` | TEXT | YES | 16,777,216 | Unique string identifier propagated from the source Iceberg table. Snowflake VARCHAR default cap applied (16M chars). Semantically identical to `ICEBERG_SCHEMA.JSON_TO_PARQUET_ICEBERG.ID`. Refreshed via full `CREATE OR REPLACE` on every hourly task run. |
| `NAME` | TEXT | YES | 16,777,216 | Name/label propagated from the source Iceberg table. Semantically identical to `ICEBERG_SCHEMA.JSON_TO_PARQUET_ICEBERG.NAME`. No transformation applied by dbt model. |

---

### `nishant_test_nonprod_db.json_to_parquet_iceberg` (AWS Glue Catalog — Iceberg Table)

| Column | Glue Type | Iceberg Field ID | Optional | Current | Description |
|--------|-----------|-----------------|----------|---------|-------------|
| `id` | string | 1 | true | true | Record identifier extracted from raw JSON source. Registered as Iceberg field ID 1 — stable across schema evolutions. `optional=true` means nulls are permitted in the Iceberg spec. |
| `name` | string | 2 | true | true | Record name extracted from raw JSON source. Registered as Iceberg field ID 2. `current=true` means this column is active in the latest schema version (not dropped). |

---

### `nishant_test_nonprod_db.json_to_parquet` (AWS Glue Catalog — Parquet Table)

| Column | Glue Type | Serialisation | Description |
|--------|-----------|--------------|-------------|
| `id` | string | `ParquetHiveSerDe` | Record identifier written as Snappy-compressed Parquet. Same value as the Iceberg `id` column — written in parallel by the same Glue job step. |
| `name` | string | `ParquetHiveSerDe` | Record name written as Snappy-compressed Parquet. Same value as the Iceberg `name` column. |

---

### `MICRO_FUTURES.MCL_USD_BARREL_FORECAST` (Snowflake — MCL Forecast Table)

| Column | Data Type | Description |
|--------|-----------|-------------|
| `FORECAST_DATE` | DATE | Trading day for which the forecast applies. Next 10 weekdays from forecast generation date. Primary key. |
| `PREDICTED_OPEN` | DECIMAL(10,2) | Projected opening price in USD/barrel, estimated from prior day's close with small Gaussian noise (σ = 0.15 × ATR). |
| `PREDICTED_HIGH` | DECIMAL(10,2) | Projected intraday high. Calculated as `max(open, close) + ATR × U(0.3, 0.65)` where U is a uniform random draw seeded per day for reproducibility. |
| `PREDICTED_LOW` | DECIMAL(10,2) | Projected intraday low. Calculated as `min(open, close) − ATR × U(0.3, 0.65)`. |
| `PREDICTED_CLOSE` | DECIMAL(10,2) | Core forecast price. Derived from 20-day linear regression projection, trend-damped using a decay factor (100% → 10% over 10 days), with bounded noise overlay. |
| `BUY_POINT` | DECIMAL(10,2) | Recommended long entry price. Defined as `max(Support_1, Fib_61.8%) + 0.05 × ATR`. Places entry just above the strongest support confluence. |
| `SELL_POINT` | DECIMAL(10,2) | Recommended exit / short entry price. Defined as `min(Resistance_1, 30d_high × 1.01) − 0.05 × ATR`. Places exit just below nearest resistance. |
| `STOP_LOSS` | DECIMAL(10,2) | Hard stop price for long trades. Set at `Buy_Point − 1.5 × ATR`. Represents maximum acceptable loss per trade. |
| `TAKE_PROFIT` | DECIMAL(10,2) | Aggressive take-profit target for long trades. Set at `Sell_Point + 0.5 × ATR`. Used for optimistic exit scenarios. |
| `PIVOT_POINT` | DECIMAL(10,2) | Classic pivot point: `(High + Low + Close) / 3`. The fulcrum level — price above PP is considered bullish bias, below is bearish. |
| `SUPPORT_1` | DECIMAL(10,2) | First support level: `2 × PP − High`. First bounce zone below current price. |
| `SUPPORT_2` | DECIMAL(10,2) | Second support level: `PP − (High − Low)`. Stronger support; a break below S2 signals trend reversal. |
| `SUPPORT_3` | DECIMAL(10,2) | Third support level: `Low − 2 × (High − PP)`. Deep support — rarely reached in normal conditions. |
| `RESISTANCE_1` | DECIMAL(10,2) | First resistance level: `2 × PP − Low`. First ceiling zone above current price. |
| `RESISTANCE_2` | DECIMAL(10,2) | Second resistance level: `PP + (High − Low)`. Stronger resistance; breakout above R2 signals momentum continuation. |
| `RESISTANCE_3` | DECIMAL(10,2) | Third resistance level: `High + 2 × (PP − Low)`. Deep resistance — breakout above R3 is a strong bullish signal. |
| `FIB_382` | DECIMAL(10,2) | 38.2% Fibonacci retracement from 30-day swing low to swing high. Key retracement level in trend continuations. |
| `FIB_618` | DECIMAL(10,2) | 61.8% Fibonacci retracement ("golden ratio"). Strongest Fibonacci support — deep pullback before trend resumes. |
| `ATR` | DECIMAL(10,2) | Projected Average True Range. Estimated from 14-day return volatility × current price. Expands slightly over the forecast horizon. Used for stop/target sizing. |
| `RSI_PROJECTED` | DECIMAL(5,1) | Projected RSI(14). Starts from the last observed RSI value and decays toward 50 at 3.5 points/day, modelling mean reversion from overbought conditions. |
| `EMA9_REF` | DECIMAL(10,2) | EMA-9 value at time of forecast creation. Reference only — not re-projected daily. Used to assess short-term trend momentum. |
| `EMA21_REF` | DECIMAL(10,2) | EMA-21 value at time of forecast creation. Reference only. When EMA9 > EMA21, short-term trend is bullish (golden cross). |
| `BB_UPPER` | DECIMAL(10,2) | Bollinger Band upper boundary (20-period, 2 standard deviations) at forecast creation time. Price above BB Upper = statistically overbought. |
| `BB_LOWER` | DECIMAL(10,2) | Bollinger Band lower boundary (20-period, 2 standard deviations) at forecast creation time. Price below BB Lower = statistically oversold. |
| `SIGNAL` | VARCHAR(10) | Trading signal: `BUY` (bullish momentum, RSI < 70), `SELL` (RSI > 70 overbought or bearish momentum), `HOLD` (neutral, no clear edge). |
| `TREND` | VARCHAR(20) | Market condition label: `BULLISH`, `BEARISH`, `NEUTRAL`, `OVERBOUGHT`, `OVERSOLD`. Derived from RSI decay projection and price vs. EMA. |
| `CONFIDENCE_PCT` | DECIMAL(5,1) | Signal confidence score (0–100%). Higher confidence = stronger momentum evidence. Bounded at 92% to avoid overconfidence. |
| `RISK_REWARD_RATIO` | DECIMAL(5,2) | `(Sell_Point − Buy_Point) / (Buy_Point − Stop_Loss)`. A ratio ≥ 1.0 is acceptable; ≥ 2.0 is preferred. |
| `BASE_PRICE_USED` | DECIMAL(10,2) | Last actual EIA RWTC spot price used as anchor for the forecast model. Sourced from `api.eia.gov`. |
| `DATA_SOURCE` | VARCHAR(200) | Description of the data source and model methodology used to generate this row. |
| `CREATED_AT` | TIMESTAMP_NTZ | UTC timestamp when this forecast row was inserted into Snowflake. |

---

## Data Glossary

### Business Terms

| Term | Definition |
|------|-----------|
| **Iceberg Table** | An open-source table format (Apache Iceberg) that adds ACID transactions, schema evolution, time travel, and partition evolution on top of object storage (S3). Supports both AWS Glue and Snowflake as query engines simultaneously. |
| **Data Lineage** | The complete record of where data originates, how it moves, and how it transforms across systems — from raw source to final consumption. Used for impact analysis, debugging, compliance, and trust. |
| **ETL** | Extract, Transform, Load. The process of reading data from a source (JSON on S3), transforming it (selecting columns, casting types), and loading it to a destination (Parquet + Iceberg). |
| **dbt (data build tool)** | A transformation framework that runs SQL-based models inside the data warehouse. In this pipeline, dbt reads the source Iceberg table and creates a refreshed output table on a schedule. |
| **Glue Catalog** | AWS Glue Data Catalog — a centralised metadata repository that stores table definitions (schema, location, format) for use by Athena, Glue ETL, Spark, and Snowflake. |
| **Snapshot (Iceberg)** | An immutable point-in-time view of an Iceberg table. Each write operation creates a new snapshot. Enables time-travel queries (`AS OF`) and rollback. Snapshot ID: `9076158952705182747`. |
| **Manifest (Iceberg)** | An Avro file listing all data files that belong to a specific Iceberg snapshot, along with file statistics (row counts, null counts, min/max values). |
| **Parquet** | A columnar binary file format optimised for analytical queries. Stores data column-by-column, enabling efficient reads for specific fields. Used here with Snappy compression. |
| **Pivot Point** | A technical analysis price level calculated from the prior period's high, low, and close. Divides the market into bullish (above PP) and bearish (below PP) regimes. |
| **ATR (Average True Range)** | A volatility indicator measuring average daily price range. Larger ATR = more volatile market. Used here to size stop losses and price targets proportionally. |
| **RSI (Relative Strength Index)** | A momentum oscillator (0–100). RSI > 70 = overbought (potential reversal), RSI < 30 = oversold (potential bounce). Developed by J. Welles Wilder. |
| **EMA (Exponential Moving Average)** | A moving average that weights recent prices more heavily than older ones. EMA-9 reacts faster to price changes than EMA-21. A 9/21 EMA crossover signals trend changes. |
| **Bollinger Bands** | Volatility bands placed 2 standard deviations above/below a 20-period moving average. When price approaches the upper band, the asset is considered statistically overbought. |
| **Fibonacci Retracement** | Horizontal support/resistance levels at 23.6%, 38.2%, 50%, 61.8%, and 78.6% of a prior price swing. The 61.8% level ("golden ratio") is considered the strongest. |
| **Risk:Reward Ratio (R:R)** | `Potential profit / Potential loss`. A 2:1 R:R means you risk $1 to make $2. Generally, R:R ≥ 1.5 is considered acceptable in trading. |
| **MCL** | Micro WTI Crude Oil futures contract (CME Group). 1/10th the size of a standard CL contract. Tracks the price of West Texas Intermediate (WTI) crude oil in USD per barrel. |
| **WTI (West Texas Intermediate)** | A grade of crude oil used as a pricing benchmark for North American oil markets. Spot price published daily by the U.S. Energy Information Administration (EIA). |
| **EIA RWTC** | U.S. Energy Information Administration — Cushing, OK WTI Spot Price FOB (Dollars per Barrel). The authoritative daily spot price for WTI crude oil. Series ID: `RWTC`. |

### Technical Terms

| Term | Definition |
|------|-----------|
| **S3 (Simple Storage Service)** | AWS object storage service. Used here as the persistent storage layer for raw JSON input, Parquet output, Iceberg data files, and metadata files. |
| **AWS Glue** | Serverless ETL service on AWS. Runs PySpark jobs to read, transform, and write data. Manages the Data Catalog (schema registry). |
| **Glue Job Bookmark** | A Glue feature that tracks which data has already been processed. Prevents re-processing files already ingested in previous job runs. Enabled in this pipeline. |
| **SNOWFLAKE_S3_INTEGRATION** | A Snowflake storage integration object that uses an AWS IAM role to authenticate S3 access without embedding credentials in Snowflake. |
| **External Stage (Snowflake)** | A named reference to an S3 location in Snowflake. `ICEBERG_S3_STAGE` points to `s3://nishant-test-nonprod-iceberg/` and is used to register the Iceberg table. |
| **GitHub Actions** | CI/CD platform used to trigger the Glue job. The `GitHubActionsRole` IAM role created both the Glue Catalog tables and triggers pipeline runs. |
| **Snappy Compression** | A fast, lossless compression algorithm used for the Parquet files. Balances compression ratio (~2:1) with read/write speed. |
| **MSCK REPAIR TABLE** | An Athena/Hive DDL command that scans S3 and adds any new partitions to the Glue Catalog. Required after writing new Parquet files to partitioned S3 paths. |
| **Spark Catalog (glue_catalog)** | Iceberg's Spark integration that connects to AWS Glue as the Iceberg catalog. Configured via `spark.sql.catalog.glue_catalog.*` properties. |
| **DPU (Data Processing Unit)** | The unit of compute capacity in AWS Glue. G.1X workers = 1 DPU each (4 vCPU, 16 GB RAM). This job uses 2 workers = 2 DPUs. |
| **VertiPaq / Snowflake micro-partition** | Snowflake's internal columnar storage format. Iceberg tables in Snowflake read directly from S3 Parquet files rather than using Snowflake's internal storage. |
| **CRON schedule** | Time-based task scheduling syntax. `CRON 0 * * * * UTC` = "at minute 0 of every hour, every day, UTC timezone". |
| **Avro** | A row-based binary serialisation format. Used by Iceberg for manifest files and snapshot files (metadata layer). |
| **metadata.json (Iceberg)** | The root metadata file for an Iceberg table. Contains schema definition, partition spec, snapshot history, and pointer to the current manifest list. |
| **Parquet field ID** | A stable numeric identifier for each column in the Iceberg schema. Field IDs (`id=1`, `name=2`) are immutable across renames, allowing schema evolution without breaking downstream readers. |
| **IAM Role** | AWS Identity and Access Management Role. `nishant-test-non-prod-glue-role` grants the Glue job permissions to read/write S3 and register tables in the Glue Catalog. |

---

*Field descriptions and glossary added March 11, 2026. No column comments were set in either the AWS Glue Catalog or Snowflake at time of generation — descriptions above are authored from schema inspection and pipeline code analysis.*
