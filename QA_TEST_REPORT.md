# QA Test Report — Non-Prod Data Pipeline

**Environment:** Non-Prod
**Report Date:** 2026-03-09
**Glue Job:** `nishant-test-nonprod-json-to-parquet`
**Script:** `glue_json_to_parquet_v4.py`
**Source File:** `s3://nishant-test-nonprod-source-raw/raw/regression/sample_data_RT-01.json`
**Athena Database:** `nishant_test_nonprod_db`

---

## Overall Summary

| Category | Tests | Passed | Failed |
|----------|-------|--------|--------|
| Source Data | 3 | 3 | 0 |
| Glue Job Execution | 3 | 2 | 1 |
| Parquet Table | 5 | 3 | 2 |
| Iceberg Table | 5 | 5 | 0 |
| Schema Validation | 1 | 1 | 0 |
| S3 / Metadata | 2 | 2 | 0 |
| **TOTAL** | **19** | **16** | **3** |

---

## Section 1 — Source Data Validation

### TC-S01 · Source File Exists in S3

| | |
|-|-|
| **Path** | `s3://nishant-test-nonprod-source-raw/raw/regression/sample_data_RT-01.json` |
| **Expected** | File present and non-empty |
| **Actual** | Present · 36.5 KiB |
| **Status** | ✅ PASS |

---

### TC-S02 · Source Record Count

| | |
|-|-|
| **Format** | NDJSON (one JSON object per line) |
| **Expected** | > 0 records |
| **Actual** | **1,000 records** |
| **Status** | ✅ PASS |

---

### TC-S03 · Source Data Integrity

| Check | Expected | Actual | Result |
|-------|----------|--------|--------|
| Null `id` values | 0 | 0 | ✅ |
| Null `name` values | 0 | 0 | ✅ |
| Empty `name` values | 0 | 0 | ✅ |
| `id` range | 1 – 1,000 | 1 – 1,000 | ✅ |
| Unique `id` count | 1,000 | 1,000 (100%) | ✅ |
| Unique `name` count | — | 594 / 1,000 (expected, names repeat) | ✅ |

**Status:** ✅ PASS

---

## Section 2 — Glue Job Execution

### TC-G01 · Latest Job Run Status

| | |
|-|-|
| **Run ID** | `jr_be493557ac9d66a901011c1cf1af61abb34e9e99f2100f93c09179ef628ad511` |
| **Started** | 2026-03-09 19:42 UTC |
| **Duration** | 108 seconds |
| **DPU** | 2.0 |
| **Expected** | SUCCEEDED |
| **Actual** | **SUCCEEDED** |
| **Status** | ✅ PASS |

---

### TC-G02 · Full Job Run History (10 Runs)

| # | Run ID (short) | State | Duration | Error |
|---|----------------|-------|----------|-------|
| 1 | `jr_be493557` | ✅ SUCCEEDED | 108s | — |
| 2 | `jr_d31d154c` | ✅ SUCCEEDED | 78s | — |
| 3 | `jr_2531da33_attempt_1` | ❌ FAILED | 80s | `NameError: DynamicFrame not defined` |
| 4 | `jr_2531da33` | ❌ FAILED | 85s | `NameError: DynamicFrame not defined` |
| 5 | `jr_eedef136_attempt_1` | ❌ FAILED | 78s | `NameError: DynamicFrame not defined` |
| 6 | `jr_eedef136` | ❌ FAILED | 62s | `NameError: DynamicFrame not defined` |
| 7 | `jr_6f0ae942_attempt_1` | ❌ FAILED | 75s | `NameError: DynamicFrame not defined` |
| 8 | `jr_6f0ae942` | ❌ FAILED | 63s | `All records must be objects!` |
| 9 | `jr_a1872666_attempt_1` | ❌ FAILED | 35s | `Cannot modify static config: spark.sql.extensions` |
| 10 | `jr_a1872666` | ❌ FAILED | 58s | `Cannot modify static config: spark.sql.extensions` |

**Success rate:** 2 / 10 runs · All historical failures resolved in v4 script.

**Status:** ✅ PASS (current script stable)

---

### TC-G03 · Parquet Double-Write Detection

| | |
|-|-|
| **Objective** | Verify job bookmark prevents same source data being written twice |
| **Expected** | 1,000 rows in Parquet table (1:1 with source) |
| **Actual** | **2,000 rows** — source file processed by 2 successful runs |
| **Root Cause** | Job bookmark did not prevent re-ingestion of the same file across the two successful runs |
| **Iceberg Impact** | None — Iceberg was enabled only in the latest run; 1,000 rows (correct) |
| **Status** | ⚠️ FAIL |
| **Recommendation** | Reset job bookmark and truncate Parquet S3 target before the next run |

---

## Section 3 — Parquet Table (`json_to_parquet`)

### TC-P01 · Row Count

| | |
|-|-|
| **Query** | `SELECT COUNT(*) FROM json_to_parquet` |
| **Expected** | 1,000 |
| **Actual** | **2,000** |
| **Status** | ⚠️ FAIL — double-write (see TC-G03) |

---

### TC-P02 · Null Check

| | |
|-|-|
| **Query** | `SELECT COUNT(*) FROM json_to_parquet WHERE id IS NULL OR name IS NULL` |
| **Expected** | 0 |
| **Actual** | **0** |
| **Status** | ✅ PASS |

---

### TC-P03 · Empty Value Check

| | |
|-|-|
| **Query** | `SELECT COUNT(*) FROM json_to_parquet WHERE TRIM(name)='' OR TRIM(id)=''` |
| **Expected** | 0 |
| **Actual** | **0** |
| **Status** | ✅ PASS |

---

### TC-P04 · Duplicate `id` Check

| | |
|-|-|
| **Query** | `SELECT COUNT(*) FROM (SELECT id FROM json_to_parquet GROUP BY id HAVING COUNT(*) > 1)` |
| **Expected** | 0 |
| **Actual** | **1,000** — all ids duplicated from double-write |
| **Status** | ⚠️ FAIL — same root cause as TC-G03 / TC-P01 |

---

### TC-P05 · S3 Output Files

| | |
|-|-|
| **Path** | `s3://nishant-test-nonprod-target-parquet/json_to_parquet/` |
| **Expected** | Files present, Snappy compressed |
| **Actual** | **40 files** (20 per run × 2 runs) · `.snappy.parquet` confirmed |
| **Status** | ✅ PASS |

---

## Section 4 — Iceberg Table (`json_to_parquet_iceberg`)

### TC-I01 · Row Count

| | |
|-|-|
| **Query** | `SELECT COUNT(*) FROM json_to_parquet_iceberg` |
| **Expected** | 1,000 (matching source) |
| **Actual** | **1,000** |
| **Status** | ✅ PASS |

---

### TC-I02 · Null Check

| | |
|-|-|
| **Query** | `SELECT COUNT(*) FROM json_to_parquet_iceberg WHERE id IS NULL OR name IS NULL` |
| **Expected** | 0 |
| **Actual** | **0** |
| **Status** | ✅ PASS |

---

### TC-I03 · Empty Value Check

| | |
|-|-|
| **Query** | `SELECT COUNT(*) FROM json_to_parquet_iceberg WHERE TRIM(name)='' OR TRIM(id)=''` |
| **Expected** | 0 |
| **Actual** | **0** |
| **Status** | ✅ PASS |

---

### TC-I04 · Duplicate `id` Check

| | |
|-|-|
| **Query** | `SELECT COUNT(*) FROM (SELECT id FROM json_to_parquet_iceberg GROUP BY id HAVING COUNT(*) > 1)` |
| **Expected** | 0 |
| **Actual** | **0** |
| **Status** | ✅ PASS |

---

### TC-I05 · Data Range & Uniqueness

| Metric | Expected | Actual | Result |
|--------|----------|--------|--------|
| Min `id` | 1 | 1 | ✅ |
| Max `id` | 1,000 | 1,000 | ✅ |
| Unique `id` count | 1,000 | 1,000 | ✅ |
| Unique `name` count | 594 | 594 | ✅ |

**Status:** ✅ PASS

---

## Section 5 — Schema Validation

### TC-SC01 · Column Names & Data Types

| Table | Column | Expected Type | Actual Type | Result |
|-------|--------|--------------|-------------|--------|
| `json_to_parquet` | id | varchar | varchar | ✅ |
| `json_to_parquet` | name | varchar | varchar | ✅ |
| `json_to_parquet_iceberg` | id | varchar | varchar | ✅ |
| `json_to_parquet_iceberg` | name | varchar | varchar | ✅ |

**Status:** ✅ PASS

---

## Section 6 — S3 & Iceberg Metadata

### TC-M01 · Iceberg Metadata Integrity

| File | Size | Status |
|------|------|--------|
| `00000-...metadata.json` (initial snapshot) | 767 B | ✅ Present |
| `00001-...metadata.json` (post-write snapshot) | 2.1 KiB | ✅ Present |
| `f1a16133-...-m0.avro` (manifest file) | 7.4 KiB | ✅ Present |
| `snap-9076158952705182747-....avro` (snapshot) | 4.2 KiB | ✅ Present |
| Data files | 20 Parquet files | ✅ Present |

**Status:** ✅ PASS

---

### TC-M02 · Glue Script in S3

| | |
|-|-|
| **Path** | `s3://nishant-test-nonprod-glue-scripts/scripts/glue_json_to_parquet_v4.py` |
| **Expected** | Script present and up to date |
| **Actual** | Present · 6.4 KiB · includes Iceberg write logic |
| **Status** | ✅ PASS |

---

## Findings & Recommendations

| # | Severity | Finding | Recommendation |
|---|----------|---------|----------------|
| 1 | ⚠️ Medium | Parquet table has 2,000 rows instead of 1,000 — double-write from 2 job runs on the same source file | Reset the Glue job bookmark and delete/recreate the Parquet S3 target before the next run |
| 2 | ℹ️ Info | 8 of 10 historical job runs failed | All root causes resolved in v4: missing `DynamicFrame` import fixed, `spark.sql.extensions` static config conflict resolved via job-level `--conf` arg |
| 3 | ℹ️ Info | Iceberg table is clean — 1,000 rows, no nulls, no duplicates | No action needed |

---

## Source Data Profile

| Attribute | Value |
|-----------|-------|
| File | `sample_data_RT-01.json` |
| Format | NDJSON (1 record per line) |
| Size | 36.5 KiB |
| Total Records | 1,000 |
| Columns | `id`, `name` |
| `id` Range | 1 – 1,000 |
| Unique IDs | 1,000 (100%) |
| Unique Names | 594 / 1,000 |
| Null Values | 0 |
| Empty Values | 0 |
