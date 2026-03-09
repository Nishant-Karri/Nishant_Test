# Terraform Plan — Non-Prod Environment

**Environment:** non-prod
**Date:** 2026-03-09
**Branch:** feature/terraform-non-prod-plan
**Author:** Nishant-Karri

---

## Resources to be Created

### S3 Buckets (5)

| Bucket Name | Purpose |
|-------------|---------|
| `nishant-test-nonprod-source-raw` | Raw JSON input files |
| `nishant-test-nonprod-target-parquet` | Parquet output files |
| `nishant-test-nonprod-athena-results` | Athena query results |
| `nishant-test-nonprod-glue-scripts` | Glue PySpark scripts + test data |
| `nishant-test-nonprod-iceberg` | Iceberg table data + warehouse |

All buckets:
- Versioning enabled
- Public access blocked
- Server-side encryption (AES256)

---

### IAM Role (1)

| Resource | Value |
|----------|-------|
| Role name | `nishant-test-non-prod-glue-role` |
| Trusted service | `glue.amazonaws.com` |
| Permissions | S3, Athena, Glue Catalog, CloudWatch Logs |

---

### AWS Glue Job (1)

| Setting | Value |
|---------|-------|
| Job name | `nishant-test-nonprod-json-to-parquet` |
| Script | `glue_json_to_parquet_v4.py` |
| Glue version | 4.0 |
| Worker type | G.1X |
| Workers | 2 |
| Timeout | 60 minutes |
| Max retries | 1 |
| Iceberg enabled | Yes (`--datalake-formats=iceberg`) |
| Job bookmarks | Enabled |
| Metrics | Enabled |
| Spark UI | Enabled |

---

### Glue Catalog Database (1)

| Setting | Value |
|---------|-------|
| Database name | `nishant_test_nonprod_db` |

---

### Glue Catalog Tables (2)

| Table | Type | Format | Location |
|-------|------|--------|----------|
| `json_to_parquet` | EXTERNAL_TABLE | Parquet (Snappy) | `s3://nishant-test-nonprod-target-parquet/json_to_parquet/` |
| `json_to_parquet_iceberg` | EXTERNAL_TABLE | Iceberg | `s3://nishant-test-nonprod-iceberg/json_to_parquet_iceberg/` |

Both tables expose columns: `id STRING`, `name STRING`

---

### CloudWatch Log Group (1)

| Setting | Value |
|---------|-------|
| Log group | `/aws-glue/jobs/nishant-test-nonprod-json-to-parquet` |
| Retention | 30 days |

---

### DynamoDB Table (1)

| Setting | Value |
|---------|-------|
| Table name | `nishant-test-terraform-locks` |
| Purpose | Terraform state locking |
| Billing | Pay per request |

---

## Total Resources: 18

| Resource Type | Count |
|---------------|-------|
| `aws_s3_bucket` | 5 |
| `aws_s3_bucket_versioning` | 3 |
| `aws_s3_bucket_public_access_block` | 5 |
| `aws_s3_object` | 1 |
| `aws_iam_role` | 1 |
| `aws_iam_role_policy` | 1 |
| `aws_iam_role_policy_attachment` | 1 |
| `aws_glue_catalog_database` | 1 |
| `aws_glue_catalog_table` | 2 |
| `aws_glue_job` | 1 |
| `aws_cloudwatch_log_group` | 1 |
| `aws_dynamodb_table` | 1 |
| **Total** | **23** |

---

## Deployment Steps

```bash
# 1. Initialize
terraform init -backend-config="bucket=nishant-test-terraform-state" \
               -backend-config="key=nishant-test/non-prod/terraform.tfstate" \
               -backend-config="region=us-east-1"

# 2. Plan
terraform plan -var-file="environments/non-prod/terraform.tfvars"

# 3. Apply
terraform apply -var-file="environments/non-prod/terraform.tfvars"
```

---

## Validation Checklist

- [ ] S3 state bucket `nishant-test-terraform-state` created before init
- [ ] All 5 S3 buckets created with versioning and encryption
- [ ] Glue script uploaded to `nishant-test-nonprod-glue-scripts/scripts/`
- [ ] Glue job visible in AWS Glue console
- [ ] Athena database and both tables visible in Glue Catalog
- [ ] IAM role has correct trust policy and permissions
- [ ] CloudWatch log group created
- [ ] DynamoDB lock table created
