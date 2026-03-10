# AWS ↔ GitHub Integration Setup Guide

## Step 1 — Create IAM OIDC Provider in AWS

Run this once in your AWS account to trust GitHub:

```bash
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1
```

---

## Step 2 — Create IAM Role for GitHub Actions

```bash
aws iam create-role \
  --role-name GitHubActionsRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": { "Federated": "arn:aws:iam::<YOUR_ACCOUNT_ID>:oidc-provider/token.actions.githubusercontent.com" },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": { "token.actions.githubusercontent.com:aud": "sts.amazonaws.com" },
        "StringLike":   { "token.actions.githubusercontent.com:sub": "repo:Nishant-Karri/Nishant_Test:*" }
      }
    }]
  }'

# Attach required policies
aws iam attach-role-policy --role-name GitHubActionsRole --policy-arn arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess
aws iam attach-role-policy --role-name GitHubActionsRole --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
aws iam attach-role-policy --role-name GitHubActionsRole --policy-arn arn:aws:iam::aws:policy/AmazonAthenaFullAccess
```

---

## Step 3 — Add GitHub Secrets

Go to: **GitHub → Nishant_Test → Settings → Secrets → Actions → New repository secret**

| Secret Name | Value |
|-------------|-------|
| `AWS_ACCOUNT_ID` | Your 12-digit AWS account ID |
| `AWS_REGION` | e.g. `us-east-1` |
| `GLUE_JOB_NAME_NONPROD` | Glue job name in Non-prod |
| `GLUE_JOB_NAME_PROD` | Glue job name in Prod |
| `GLUE_SCRIPTS_BUCKET_NONPROD` | S3 bucket for Non-prod scripts |
| `GLUE_SCRIPTS_BUCKET_PROD` | S3 bucket for Prod scripts |
| `SOURCE_BUCKET_NAME` | S3 source JSON bucket |
| `TARGET_BUCKET_NAME` | S3 target Parquet bucket |
| `ATHENA_DATABASE` | Athena database name |
| `ATHENA_TABLE` | Hive Parquet table name |
| `ATHENA_OUTPUT_S3` | S3 path for Athena results |
| `ATHENA_RESULTS_BUCKET` | Bucket for Athena results |
| `ICEBERG_TABLE` | Iceberg table name |
| `ICEBERG_S3_PATH` | S3 path for Iceberg data |
| `ICEBERG_WAREHOUSE` | S3 path for Iceberg warehouse |
| `DEFAULT_SOURCE_S3_PATH` | Default source for scheduled runs |
| `DEFAULT_TARGET_S3_PATH` | Default target for scheduled runs |

---

## Workflows Summary

| Workflow | Trigger | What it does |
|----------|---------|-------------|
| `aws-oidc-setup.yml` | Manual | Verifies AWS connection is working |
| `deploy-glue-job.yml` | Push to Non-prod/Prod | Uploads Glue script to S3, updates Glue job |
| `run-glue-job.yml` | Manual / Daily 2AM | Triggers Glue job, polls until complete |
| `s3-sync-test-data.yml` | Push to Non-prod | Syncs test & regression datasets to S3 |
| `athena-validate.yml` | After Glue job / Manual | Validates Parquet + Iceberg row counts and schema |
| `terraform-deploy.yml` | PR / Push to Non-prod/Prod | Plans/applies Terraform infrastructure |
| `promote-to-prod.yml` | Manual | Creates PR from Non-prod → Prod with checklist |
