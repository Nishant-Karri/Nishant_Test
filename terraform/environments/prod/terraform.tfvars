environment        = "prod"
aws_region         = "us-east-1"
project_name       = "nishant-test"

# S3 Buckets
source_bucket_name             = "nishant-test-prod-source-raw"
target_bucket_name             = "nishant-test-prod-target-parquet"
athena_results_bucket_name     = "nishant-test-prod-athena-results"
glue_scripts_bucket_name       = "nishant-test-prod-glue-scripts"
iceberg_bucket_name            = "nishant-test-prod-iceberg"

# Glue Job
glue_job_name        = "nishant-test-prod-json-to-parquet"
glue_script_filename = "glue_json_to_parquet_v3.py"
glue_version         = "4.0"
worker_type          = "G.1X"
number_of_workers    = 4
job_timeout_minutes  = 120
max_retries          = 2

# Athena & Iceberg
athena_database = "nishant_test_prod_db"
athena_table    = "json_to_parquet"
iceberg_table   = "json_to_parquet_iceberg"

tags = {
  Environment = "prod"
  Owner       = "Nishant-Karri"
}
