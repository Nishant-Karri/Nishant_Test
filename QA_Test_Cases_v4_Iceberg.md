# QA Test Cases & Checklist — `glue_json_to_parquet_v4.py`

**Version:** v4 (Iceberg)
**Branch:** Non-prod
**Date:** 2026-03-09
**Author:** Nishant-Karri
**Status:** In Review

---

## Overview

This document covers all test cases and the QA checklist for `glue_json_to_parquet_v4.py`.
v4 extends v3 by adding Apache Iceberg table creation after `MSCK REPAIR TABLE`.

### Job Execution Flow

```
1. Read JSON from S3
2. Select id, name columns
3. Write Parquet to target S3 (Snappy)
4. MSCK REPAIR TABLE (Hive/Athena)
5. Configure Spark for Iceberg
6. Drop existing Iceberg table (if any)
7. Create Iceberg table in Glue Catalog
8. Insert data into Iceberg table
```

---

## Test Environment

| Item | Details |
|------|---------|
| Runtime | AWS Glue 4.0 (PySpark) |
| Python | 3.9+ |
| Iceberg Version | 1.2.x (bundled with Glue 4.0) |
| Test Framework | `pytest` + `unittest.mock` |
| AWS Mocking | `moto` / `unittest.mock` |
| Run Command | `pytest test_glue_v4_iceberg.py -v` |

---

## Section 1 — Job Parameter Tests

### TC-01: All required parameters present
- **Description:** Job must bootstrap successfully when all 8 parameters are provided
- **Input:** All parameters: `JOB_NAME`, `source_s3_path`, `target_s3_path`, `athena_database`, `athena_table`, `athena_output_s3`, `iceberg_table`, `iceberg_s3_path`, `iceberg_warehouse`
- **Expected:** Job initialises without error
- **Status:** ⬜ Not Run

---

### TC-02: Missing `iceberg_table` parameter
- **Description:** Job must fail at bootstrap if `--iceberg_table` is not provided
- **Input:** All parameters except `iceberg_table`
- **Expected:** `getResolvedOptions` raises an error
- **Status:** ⬜ Not Run

---

### TC-03: Missing `iceberg_s3_path` parameter
- **Description:** Job must fail at bootstrap if `--iceberg_s3_path` is not provided
- **Input:** All parameters except `iceberg_s3_path`
- **Expected:** `getResolvedOptions` raises an error
- **Status:** ⬜ Not Run

---

### TC-04: Missing `iceberg_warehouse` parameter
- **Description:** Job must fail at bootstrap if `--iceberg_warehouse` is not provided
- **Input:** All parameters except `iceberg_warehouse`
- **Expected:** `getResolvedOptions` raises an error
- **Status:** ⬜ Not Run

---

## Section 2 — Spark Iceberg Configuration Tests

### TC-05: Iceberg catalog name is `glue_catalog`
- **Description:** `spark.sql.catalog.glue_catalog` must be set to `org.apache.iceberg.spark.SparkCatalog`
- **Expected:** `spark.conf.get("spark.sql.catalog.glue_catalog") == "org.apache.iceberg.spark.SparkCatalog"`
- **Status:** ⬜ Not Run

---

### TC-06: Warehouse path is set from job parameter
- **Description:** `spark.sql.catalog.glue_catalog.warehouse` must match the `iceberg_warehouse` job argument
- **Input:** `iceberg_warehouse = "s3://my-bucket/iceberg/warehouse/"`
- **Expected:** Config value matches input
- **Status:** ⬜ Not Run

---

### TC-07: Glue Catalog impl is set correctly
- **Description:** `catalog-impl` must point to the AWS Glue implementation
- **Expected:** `spark.sql.catalog.glue_catalog.catalog-impl == "org.apache.iceberg.aws.glue.GlueCatalog"`
- **Status:** ⬜ Not Run

---

### TC-08: S3FileIO is set as the IO implementation
- **Description:** `io-impl` must use `S3FileIO` for S3 data access
- **Expected:** `spark.sql.catalog.glue_catalog.io-impl == "org.apache.iceberg.aws.s3.S3FileIO"`
- **Status:** ⬜ Not Run

---

### TC-09: Iceberg Spark extensions are registered
- **Description:** Iceberg SQL extensions must be enabled for `CREATE TABLE ... USING iceberg` to work
- **Expected:** `spark.sql.extensions` contains `IcebergSparkSessionExtensions`
- **Status:** ⬜ Not Run

---

## Section 3 — JSON Read & Column Selection Tests

### TC-10: JSON records are read from S3
- **Description:** Job must read all records from the source S3 path
- **Input:** JSON file with 100 records containing `id` and `name`
- **Expected:** `dynamic_frame.count() == 100`
- **Status:** ⬜ Not Run

---

### TC-11: Only `id` and `name` columns are selected
- **Description:** Output DataFrame must contain exactly `id` and `name`
- **Expected:** `df_selected.columns == ["id", "name"]`
- **Status:** ⬜ Not Run

---

### TC-12: Row count unchanged after column selection
- **Description:** Selecting columns must not drop any rows
- **Input:** 100 records
- **Expected:** `df_selected.count() == 100`
- **Status:** ⬜ Not Run

---

### TC-13: Missing column raises AnalysisException
- **Description:** If source JSON is missing `id` or `name`, selection must fail
- **Input:** JSON with no `name` column
- **Expected:** `pyspark.sql.utils.AnalysisException` raised
- **Status:** ⬜ Not Run

---

## Section 4 — Parquet Write Tests

### TC-14: Parquet files written to target S3 path
- **Description:** Parquet output must appear at `target_s3_path` after job runs
- **Input:** Valid DataFrame with `id` and `name`
- **Expected:** S3 path contains `.parquet` files
- **Status:** ⬜ Not Run

---

### TC-15: Parquet compression is Snappy
- **Description:** Output Parquet files must use Snappy compression
- **Expected:** File metadata shows `SNAPPY` codec
- **Status:** ⬜ Not Run

---

### TC-16: Parquet row count matches source
- **Description:** Total rows in Parquet output must equal source JSON record count
- **Expected:** Parquet row count == source count
- **Status:** ⬜ Not Run

---

## Section 5 — MSCK REPAIR TABLE Tests

### TC-17: MSCK REPAIR TABLE SQL is correct
- **Description:** Athena query must use correct backtick-quoted `database.table` syntax
- **Input:** `athena_database = "my_db"`, `athena_table = "my_table"`
- **Expected:** Query string is `MSCK REPAIR TABLE \`my_db\`.\`my_table\``
- **Status:** ⬜ Not Run

---

### TC-18: Polling continues until SUCCEEDED
- **Description:** Job must poll Athena until terminal state is reached
- **Input:** Mock sequence: `RUNNING` → `RUNNING` → `SUCCEEDED`
- **Expected:** `get_query_execution` called 3 times, state is `SUCCEEDED`
- **Status:** ⬜ Not Run

---

### TC-19: RuntimeError raised on FAILED
- **Description:** Job must raise `RuntimeError` if MSCK REPAIR fails
- **Input:** Mock state = `FAILED`, reason = `"Table not found"`
- **Expected:** `RuntimeError` raised with message containing `"FAILED"`
- **Status:** ⬜ Not Run

---

### TC-20: RuntimeError raised on CANCELLED
- **Description:** Job must raise `RuntimeError` if MSCK REPAIR is cancelled
- **Input:** Mock state = `CANCELLED`
- **Expected:** `RuntimeError` raised with message containing `"CANCELLED"`
- **Status:** ⬜ Not Run

---

### TC-21: Iceberg step does NOT run if MSCK REPAIR fails
- **Description:** Iceberg table creation must be skipped when MSCK REPAIR raises RuntimeError
- **Input:** Mock MSCK REPAIR returns `FAILED`
- **Expected:** `DROP TABLE` and `CREATE TABLE` are never called
- **Status:** ⬜ Not Run

---

## Section 6 — Iceberg Table Creation Tests

### TC-22: DROP TABLE IF EXISTS runs before CREATE TABLE
- **Description:** Existing Iceberg table must be dropped before creating a new one
- **Input:** Iceberg table already exists
- **Expected:** `spark.sql("DROP TABLE IF EXISTS ...")` is called first
- **Status:** ⬜ Not Run

---

### TC-23: DROP TABLE IF EXISTS does not fail when table is absent
- **Description:** `IF EXISTS` clause must prevent failure when table doesn't exist
- **Input:** No existing Iceberg table
- **Expected:** No exception raised, job continues
- **Status:** ⬜ Not Run

---

### TC-24: CREATE TABLE uses correct database and table name
- **Description:** SQL must reference the correct `glue_catalog.<database>.<table>`
- **Input:** `athena_database = "my_db"`, `iceberg_table = "my_iceberg"`
- **Expected:** SQL contains `glue_catalog.\`my_db\`.\`my_iceberg\``
- **Status:** ⬜ Not Run

---

### TC-25: CREATE TABLE uses `USING iceberg`
- **Description:** Table must be declared as Iceberg format
- **Expected:** SQL contains `USING iceberg`
- **Status:** ⬜ Not Run

---

### TC-26: Iceberg table LOCATION matches `iceberg_s3_path`
- **Description:** `LOCATION` in CREATE TABLE must match the job parameter
- **Input:** `iceberg_s3_path = "s3://my-bucket/iceberg/my_table/"`
- **Expected:** SQL `LOCATION` value matches input
- **Status:** ⬜ Not Run

---

### TC-27: Iceberg table schema contains `id` and `name` columns
- **Description:** Created table must define `id STRING` and `name STRING`
- **Expected:** `SHOW COLUMNS` in Iceberg table returns `id`, `name`
- **Status:** ⬜ Not Run

---

### TC-28: Iceberg table properties are set correctly
- **Description:** TBLPROPERTIES must include correct format and compression settings
- **Expected:**
  - `table_type = ICEBERG`
  - `format = parquet`
  - `write.parquet.compression-codec = snappy`
  - `write.metadata.delete-after-commit.enabled = true`
  - `write.metadata.previous-versions-max = 10`
- **Status:** ⬜ Not Run

---

### TC-29: Data is inserted into Iceberg table after creation
- **Description:** `df_selected.writeTo(...).append()` must be called after table creation
- **Input:** DataFrame with 50 records
- **Expected:** Iceberg table row count == 50
- **Status:** ⬜ Not Run

---

### TC-30: Iceberg row count matches source DataFrame
- **Description:** Row count after insert must equal `df_selected.count()`
- **Input:** 1000 JSON records
- **Expected:** `spark.table(...).count() == 1000`
- **Status:** ⬜ Not Run

---

## Section 7 — Idempotency & Re-run Tests

### TC-31: Job is idempotent — second run replaces data cleanly
- **Description:** Running the job twice must not duplicate data in the Iceberg table
- **Input:** Run job with same parameters twice
- **Expected:** Final Iceberg row count == source record count (not doubled)
- **Status:** ⬜ Not Run

---

### TC-32: Parquet target is overwritten on re-run
- **Description:** Re-running the job must not accumulate duplicate Parquet files
- **Input:** Run job twice with same `target_s3_path`
- **Expected:** Parquet row count equals source (no duplicates)
- **Status:** ⬜ Not Run

---

### TC-33: Iceberg metadata version count does not exceed 10
- **Description:** `write.metadata.previous-versions-max = 10` must limit metadata files
- **Input:** Run job 15 times
- **Expected:** Metadata folder contains ≤ 11 version files
- **Status:** ⬜ Not Run

---

## Section 8 — Athena Query Tests

### TC-34: Iceberg table is queryable via Athena after job
- **Description:** A `SELECT *` query on the Iceberg table via Athena must return results
- **Expected:** Query succeeds, results contain `id` and `name` columns
- **Status:** ⬜ Not Run

---

### TC-35: Athena returns correct row count for Iceberg table
- **Description:** `SELECT COUNT(*) FROM <iceberg_table>` must match source JSON count
- **Expected:** Count matches
- **Status:** ⬜ Not Run

---

## Test Status Summary

| Section | Total | Passed | Failed | Not Run |
|---------|-------|--------|--------|---------|
| Job Parameter Tests | 4 | 0 | 0 | 4 |
| Spark Iceberg Configuration | 5 | 0 | 0 | 5 |
| JSON Read & Column Selection | 4 | 0 | 0 | 4 |
| Parquet Write | 3 | 0 | 0 | 3 |
| MSCK REPAIR TABLE | 5 | 0 | 0 | 5 |
| Iceberg Table Creation | 9 | 0 | 0 | 9 |
| Idempotency & Re-run | 3 | 0 | 0 | 3 |
| Athena Query | 2 | 0 | 0 | 2 |
| **Total** | **35** | **0** | **0** | **35** |

---

## QA Checklist

### Pre-Deployment

- [ ] **QA-01** Confirm Glue job version is set to **4.0** (required for Iceberg support)
- [ ] **QA-02** Verify `--datalake-formats=iceberg` is added to Glue job parameters
- [ ] **QA-03** Verify `--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions` is set
- [ ] **QA-04** Confirm all 8 job parameters are configured in the Glue job definition
- [ ] **QA-05** Ensure the Glue IAM role has `glue:CreateTable`, `glue:DeleteTable`, `glue:GetTable` permissions
- [ ] **QA-06** Ensure the Glue IAM role has S3 read/write access to `iceberg_s3_path` and `iceberg_warehouse`
- [ ] **QA-07** Confirm source S3 JSON files exist and contain `id` and `name` fields
- [ ] **QA-08** Confirm target S3 bucket exists and is writable

### Execution

- [ ] **QA-09** Run the Glue job in Non-prod with sample data and verify it completes with status `SUCCEEDED`
- [ ] **QA-10** Verify CloudWatch logs show all 8 job steps printed (read → select → write → MSCK → Iceberg)
- [ ] **QA-11** Confirm MSCK REPAIR TABLE completes with Athena state `SUCCEEDED` in logs
- [ ] **QA-12** Confirm `DROP TABLE IF EXISTS` log line appears before `CREATE TABLE`
- [ ] **QA-13** Confirm `Iceberg table created` log line appears after CREATE TABLE
- [ ] **QA-14** Confirm `Iceberg table row count` in logs matches source JSON record count

### Post-Execution Validation

- [ ] **QA-15** Check Parquet files exist in `target_s3_path` on S3
- [ ] **QA-16** Check Iceberg data files exist in `iceberg_s3_path` on S3
- [ ] **QA-17** Check Iceberg metadata folder (`iceberg_s3_path/metadata/`) contains `.json` and `.avro` files
- [ ] **QA-18** Verify Iceberg table is registered in AWS Glue Catalog under `athena_database`
- [ ] **QA-19** Query Iceberg table in Athena: `SELECT * FROM <iceberg_table> LIMIT 10` — must return results
- [ ] **QA-20** Verify Athena `SELECT COUNT(*)` matches source JSON record count
- [ ] **QA-21** Verify Iceberg table columns in Athena are `id (string)` and `name (string)`

### Idempotency

- [ ] **QA-22** Re-run the job with the same parameters — confirm job succeeds again
- [ ] **QA-23** After re-run, confirm Iceberg table row count is NOT doubled
- [ ] **QA-24** After re-run, confirm only one version of data exists (no duplicates)

### Regression (vs v3)

- [ ] **QA-25** Confirm Parquet output from v4 matches v3 output in schema and row count
- [ ] **QA-26** Confirm MSCK REPAIR TABLE behaviour is unchanged from v3
- [ ] **QA-27** Confirm existing Hive-style Parquet table in Athena is unaffected by Iceberg creation

### Sign-off

- [ ] **QA-28** All 35 test cases executed and results recorded
- [ ] **QA-29** All critical (pre-deployment + execution) checklist items passed
- [ ] **QA-30** QA Lead approval obtained before promoting to Prod branch

---

## Sign-off

| Role | Name | Date | Status |
|------|------|------|--------|
| QA Engineer | | | ⬜ Pending |
| QA Lead | | | ⬜ Pending |
| Developer | | | ⬜ Pending |
