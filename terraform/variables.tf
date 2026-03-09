variable "aws_region" {
  description = "AWS region to deploy all resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment: non-prod or prod"
  type        = string
  validation {
    condition     = contains(["non-prod", "prod"], var.environment)
    error_message = "environment must be 'non-prod' or 'prod'."
  }
}

variable "project_name" {
  description = "Project name prefix for all resource names"
  type        = string
  default     = "nishant-test"
}

# ── S3 ───────────────────────────────────────────────────────────────────────
variable "source_bucket_name" {
  description = "S3 bucket name for raw JSON input"
  type        = string
}

variable "target_bucket_name" {
  description = "S3 bucket name for Parquet output"
  type        = string
}

variable "athena_results_bucket_name" {
  description = "S3 bucket for Athena query results"
  type        = string
}

variable "glue_scripts_bucket_name" {
  description = "S3 bucket for Glue PySpark scripts and test data"
  type        = string
}

variable "iceberg_bucket_name" {
  description = "S3 bucket for Iceberg table data and warehouse"
  type        = string
}

# ── Glue ─────────────────────────────────────────────────────────────────────
variable "glue_job_name" {
  description = "Name of the AWS Glue job"
  type        = string
}

variable "glue_script_filename" {
  description = "Glue PySpark script filename (e.g. glue_json_to_parquet_v4.py)"
  type        = string
  default     = "glue_json_to_parquet_v4.py"
}

variable "glue_version" {
  description = "AWS Glue version"
  type        = string
  default     = "4.0"
}

variable "worker_type" {
  description = "Glue worker type: G.1X, G.2X, or G.025X"
  type        = string
  default     = "G.1X"
}

variable "number_of_workers" {
  description = "Number of Glue workers"
  type        = number
  default     = 2
}

variable "job_timeout_minutes" {
  description = "Glue job timeout in minutes"
  type        = number
  default     = 60
}

variable "max_retries" {
  description = "Max Glue job retries on failure"
  type        = number
  default     = 1
}

# ── Athena & Iceberg ─────────────────────────────────────────────────────────
variable "athena_database" {
  description = "Glue/Athena database name"
  type        = string
}

variable "athena_table" {
  description = "Hive-style Parquet table name"
  type        = string
  default     = "json_to_parquet"
}

variable "iceberg_table" {
  description = "Apache Iceberg table name"
  type        = string
  default     = "json_to_parquet_iceberg"
}

# ── Tags ──────────────────────────────────────────────────────────────────────
variable "tags" {
  description = "Additional tags to apply to all resources"
  type        = map(string)
  default     = {}
}
