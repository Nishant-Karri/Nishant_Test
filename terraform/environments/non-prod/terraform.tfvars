environment        = "non-prod"
aws_region         = "us-east-1"
project_name       = "nishant-test"

# S3 Buckets
source_bucket_name             = "nishant-test-nonprod-source-raw"
target_bucket_name             = "nishant-test-nonprod-target-parquet"
athena_results_bucket_name     = "nishant-test-nonprod-athena-results"
glue_scripts_bucket_name       = "nishant-test-nonprod-glue-scripts"
iceberg_bucket_name            = "nishant-test-nonprod-iceberg"

# Glue Job — v2: workers increased to 4, timeout 90 mins, retries 2
glue_job_name        = "nishant-test-nonprod-json-to-parquet"
glue_script_filename = "glue_json_to_parquet_v4.py"
glue_version         = "4.0"
worker_type          = "G.1X"
number_of_workers    = 4
job_timeout_minutes  = 90
max_retries          = 2

# Athena & Iceberg
athena_database = "nishant_test_nonprod_db"
athena_table    = "json_to_parquet"
iceberg_table   = "json_to_parquet_iceberg"

tags = {
  Environment = "non-prod"
  Owner       = "Nishant-Karri"
  CostCenter  = "engineering"
  Terraform   = "true"
  Version     = "v2"
}
