# Regression Test Cases — `glue_json_to_parquet_v4.py` vs `glue_json_to_parquet_v3.py`

**Version:** v4 vs v3
**Branch:** Non-prod
**Date:** 2026-03-09
**Author:** Nishant-Karri
**Status:** In Review

---

## Purpose

This document verifies that all existing functionality from **v3** continues to work correctly in **v4**.
Regression testing ensures the new Iceberg step did not break, alter, or degrade any previously working behaviour.

---

## Scope

| Area | v3 Behaviour | v4 Change | Regression Risk |
|------|-------------|-----------|-----------------|
| JSON Read from S3 | ✅ Working | No change | Low |
| Column Selection (`id`, `name`) | ✅ Working | No change | Low |
| Parquet Write to S3 | ✅ Working | No change | Low |
| Snappy Compression | ✅ Working | No change | Low |
| MSCK REPAIR TABLE | ✅ Working | No change | Medium — Iceberg step runs after |
| Job Parameters (6) | ✅ Working | 3 new params added | Medium |
| Athena Hive Table | ✅ Working | No change | Medium — Iceberg table is separate |
| Error Handling | ✅ Working | No change | Low |
| Job Commit | ✅ Working | No change | Low |

---

## Section 1 — JSON Read Regression

### RT-01: JSON records load correctly from S3 (same as v3)
- **Description:** Source JSON must be read identically in v4 as in v3
- **Input:** Same JSON file used in v3 testing
- **v3 Expected:** Records loaded, count printed
- **v4 Expected:** Identical behaviour — same count, same schema
- **Pass Criteria:** `dynamic_frame.count()` matches v3 result
- **Status:** ⬜ Not Run

---

### RT-02: Multi-line JSON is handled correctly
- **Description:** `multiLine=true` option must remain in effect in v4
- **Input:** Pretty-printed multi-line JSON file
- **v3 Expected:** Records parsed successfully
- **v4 Expected:** Same result — `format_options={"multiLine": "true"}` unchanged
- **Pass Criteria:** No parse errors, row count matches
- **Status:** ⬜ Not Run

---

### RT-03: Recursive S3 path reading works
- **Description:** `recurse=True` in connection options must still read nested S3 prefixes
- **Input:** JSON files spread across multiple S3 subfolders
- **v3 Expected:** All files read
- **v4 Expected:** Same behaviour
- **Pass Criteria:** All records from all subfolders are loaded
- **Status:** ⬜ Not Run

---

### RT-04: Schema is printed after read
- **Description:** `dynamic_frame.printSchema()` log line must appear in CloudWatch
- **v3 Expected:** Schema printed to logs
- **v4 Expected:** Same log output
- **Pass Criteria:** Schema log present in CloudWatch
- **Status:** ⬜ Not Run

---

## Section 2 — Column Selection Regression

### RT-05: Only `id` and `name` columns selected
- **Description:** v4 must not change the column selection logic from v3
- **Input:** JSON with extra fields (`email`, `age`, `address`)
- **v3 Expected:** Output contains only `id`, `name`
- **v4 Expected:** Identical — extra columns dropped
- **Pass Criteria:** `df_selected.columns == ["id", "name"]`
- **Status:** ⬜ Not Run

---

### RT-06: Row count unchanged after column selection
- **Description:** Selecting columns must not drop rows in v4
- **Input:** 1000 JSON records
- **v3 Expected:** `df_selected.count() == 1000`
- **v4 Expected:** Same
- **Pass Criteria:** Row counts match between v3 and v4
- **Status:** ⬜ Not Run

---

### RT-07: Column values are identical between v3 and v4
- **Description:** The actual `id` and `name` values in the output must match v3 exactly
- **Input:** Known fixed dataset (same used in v3 testing)
- **v3 Expected:** Specific `id` and `name` values
- **v4 Expected:** Identical values
- **Pass Criteria:** Row-by-row comparison passes
- **Status:** ⬜ Not Run

---

### RT-08: Null values in `id` or `name` are handled the same way
- **Description:** v4 must preserve null handling behaviour from v3
- **Input:** Records with `null` in `id` or `name`
- **v3 Expected:** Nulls retained as-is
- **v4 Expected:** Same behaviour
- **Pass Criteria:** Null rows present in output, count unchanged
- **Status:** ⬜ Not Run

---

## Section 3 — Parquet Write Regression

### RT-09: Parquet files are written to the same target path
- **Description:** `target_s3_path` behaviour must be identical in v4
- **Input:** Same `target_s3_path` as used in v3
- **v3 Expected:** Parquet files at target path
- **v4 Expected:** Same path, same files
- **Pass Criteria:** S3 path contains `.parquet` files after job
- **Status:** ⬜ Not Run

---

### RT-10: Parquet output uses Snappy compression
- **Description:** Compression codec must remain `snappy` in v4
- **v3 Expected:** Snappy-compressed Parquet files
- **v4 Expected:** Same compression
- **Pass Criteria:** File metadata shows `SNAPPY` codec
- **Status:** ⬜ Not Run

---

### RT-11: Glue Parquet writer is used
- **Description:** `useGlueParquetWriter=true` must remain enabled in v4
- **v3 Expected:** Glue optimised writer used
- **v4 Expected:** Same setting unchanged
- **Pass Criteria:** No fallback to standard Parquet writer
- **Status:** ⬜ Not Run

---

### RT-12: Parquet row count matches source
- **Description:** Total Parquet output rows must equal source JSON record count in v4
- **Input:** 1000 JSON records
- **v3 Expected:** 1000 rows in Parquet
- **v4 Expected:** Same
- **Pass Criteria:** `parquet_count == source_count`
- **Status:** ⬜ Not Run

---

### RT-13: Parquet schema is unchanged from v3
- **Description:** Parquet file schema must contain the same columns as v3 output
- **v3 Expected:** Schema: `id STRING, name STRING`
- **v4 Expected:** Identical schema
- **Pass Criteria:** Parquet schema matches exactly
- **Status:** ⬜ Not Run

---

## Section 4 — MSCK REPAIR TABLE Regression

### RT-14: MSCK REPAIR TABLE still executes in v4
- **Description:** The MSCK step must not be skipped, reordered, or altered in v4
- **v3 Expected:** Athena query executed, state polled, `SUCCEEDED`
- **v4 Expected:** Same behaviour — MSCK runs before Iceberg step
- **Pass Criteria:** CloudWatch log shows `MSCK REPAIR TABLE complete.`
- **Status:** ⬜ Not Run

---

### RT-15: MSCK REPAIR TABLE SQL syntax is unchanged
- **Description:** The SQL query string format must be identical to v3
- **v3 Expected:** `` MSCK REPAIR TABLE `db`.`table` ``
- **v4 Expected:** Same SQL
- **Pass Criteria:** Query string matches v3 exactly
- **Status:** ⬜ Not Run

---

### RT-16: Athena polling logic is unchanged
- **Description:** The `while True` polling loop must behave identically to v3
- **v3 Expected:** Polls every 5 seconds, stops on terminal state
- **v4 Expected:** Same polling behaviour
- **Pass Criteria:** Same number of poll calls for same mock sequence
- **Status:** ⬜ Not Run

---

### RT-17: RuntimeError on FAILED state still raised
- **Description:** v4 must still raise `RuntimeError` when MSCK state is `FAILED`
- **v3 Expected:** `RuntimeError` raised with reason
- **v4 Expected:** Same error raised — Iceberg step never runs
- **Pass Criteria:** Exception message matches v3 format: `MSCK REPAIR TABLE FAILED: <reason>`
- **Status:** ⬜ Not Run

---

### RT-18: RuntimeError on CANCELLED state still raised
- **Description:** v4 must still raise `RuntimeError` when MSCK state is `CANCELLED`
- **v3 Expected:** `RuntimeError` raised
- **v4 Expected:** Same
- **Pass Criteria:** Exception raised, Iceberg step skipped
- **Status:** ⬜ Not Run

---

### RT-19: Hive-style Athena Parquet table is unaffected by Iceberg
- **Description:** The existing Hive-style Parquet table registered in Athena must not be dropped, altered, or overwritten by the Iceberg step
- **v3 Expected:** Hive table remains intact
- **v4 Expected:** Iceberg creates a separate table — Hive table untouched
- **Pass Criteria:** Hive table still queryable, row count unchanged after v4 run
- **Status:** ⬜ Not Run

---

## Section 5 — Job Parameter Regression

### RT-20: Existing 6 v3 parameters still work in v4
- **Description:** The original 6 parameters must be accepted without change in v4
- **Input:** `JOB_NAME`, `source_s3_path`, `target_s3_path`, `athena_database`, `athena_table`, `athena_output_s3`
- **v3 Expected:** All 6 parsed correctly
- **v4 Expected:** Same — plus 3 new Iceberg params
- **Pass Criteria:** No `KeyError` on any of the original 6 parameters
- **Status:** ⬜ Not Run

---

### RT-21: Job name initialisation is unchanged
- **Description:** `job.init(args["JOB_NAME"], args)` must work the same as v3
- **v3 Expected:** Job bookmarks initialised correctly
- **v4 Expected:** Same
- **Pass Criteria:** No error at job init
- **Status:** ⬜ Not Run

---

## Section 6 — Error Handling & Job Commit Regression

### RT-22: Job commits correctly on success
- **Description:** `job.commit()` must be called at the end of v4 just as in v3
- **v3 Expected:** Job bookmark updated, Glue job status = `SUCCEEDED`
- **v4 Expected:** Same — `job.commit()` at end of script
- **Pass Criteria:** Glue console shows job run as `Succeeded`
- **Status:** ⬜ Not Run

---

### RT-23: Job does not commit on MSCK REPAIR failure
- **Description:** If MSCK REPAIR raises `RuntimeError`, `job.commit()` must not be called
- **v3 Expected:** Job fails before commit
- **v4 Expected:** Same — exception propagates, no commit
- **Pass Criteria:** Glue job status = `Failed`, no bookmark advancement
- **Status:** ⬜ Not Run

---

### RT-24: CloudWatch log output is consistent with v3
- **Description:** All existing log print statements from v3 must still appear in v4 logs
- **v3 Log Lines Expected:**
  - `Reading JSON from: <path>`
  - `Record count: <n>`
  - `Selected columns: id, name`
  - `Row count after select: <n>`
  - `Writing Parquet to: <path>`
  - `Running MSCK REPAIR TABLE on <db>.<table>`
  - `Athena query state: SUCCEEDED`
  - `MSCK REPAIR TABLE complete.`
  - `Job complete.`
- **v4 Expected:** All above lines present, plus new Iceberg log lines
- **Pass Criteria:** All v3 log lines present in v4 CloudWatch output
- **Status:** ⬜ Not Run

---

## Section 7 — End-to-End Regression

### RT-25: Full job run produces same Parquet output as v3
- **Description:** Run v3 and v4 against the same source JSON. Parquet outputs must be identical
- **Input:** Identical source JSON file
- **v3 Expected:** Parquet output at `target_s3_path`
- **v4 Expected:** Identical Parquet output (same schema, same rows, same values)
- **Pass Criteria:** `diff` of row data shows no differences
- **Status:** ⬜ Not Run

---

### RT-26: Athena Parquet table query returns same results in v4 as v3
- **Description:** Querying the Hive-style Parquet table via Athena must return identical results after v4 run
- **Input:** Same dataset
- **v3 Expected:** `SELECT * FROM <athena_table>` returns N rows
- **v4 Expected:** Same result
- **Pass Criteria:** Row count and values match between v3 and v4 Athena query results
- **Status:** ⬜ Not Run

---

### RT-27: Job run duration is not significantly longer in v4
- **Description:** v4 adds an Iceberg step — total runtime should not increase by more than 20% for standard datasets
- **Input:** 100,000 JSON records
- **v3 Expected:** Baseline runtime recorded
- **v4 Expected:** Runtime ≤ v3 baseline × 1.20
- **Pass Criteria:** Duration within acceptable threshold
- **Status:** ⬜ Not Run

---

## Regression Test Summary

| Section | Total | Passed | Failed | Not Run |
|---------|-------|--------|--------|---------|
| JSON Read | 4 | 0 | 0 | 4 |
| Column Selection | 4 | 0 | 0 | 4 |
| Parquet Write | 5 | 0 | 0 | 5 |
| MSCK REPAIR TABLE | 6 | 0 | 0 | 6 |
| Job Parameters | 2 | 0 | 0 | 2 |
| Error Handling & Job Commit | 3 | 0 | 0 | 3 |
| End-to-End | 3 | 0 | 0 | 3 |
| **Total** | **27** | **0** | **0** | **27** |

---

## Regression Pass Criteria

All of the following must be true before promoting v4 to **Prod**:

- [ ] All 27 regression test cases pass
- [ ] Zero regressions in Parquet output (schema, row count, values)
- [ ] Zero regressions in MSCK REPAIR TABLE behaviour
- [ ] Hive-style Athena table unaffected by Iceberg creation
- [ ] v4 job runtime within 20% of v3 baseline
- [ ] All v3 CloudWatch log lines present in v4 output

---

## Sign-off

| Role | Name | Date | Signature |
|------|------|------|-----------|
| QA Engineer | | | |
| QA Lead | | | |
| Developer | | | |
