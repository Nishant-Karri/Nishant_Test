# ---------------------------------------------------------------------------
# S3 Buckets
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "source" {
  bucket = var.source_bucket_name
  tags   = merge(var.tags, { Name = var.source_bucket_name, Purpose = "raw-json-input" })
}

resource "aws_s3_bucket" "target" {
  bucket = var.target_bucket_name
  tags   = merge(var.tags, { Name = var.target_bucket_name, Purpose = "parquet-output" })
}

resource "aws_s3_bucket" "athena_results" {
  bucket = var.athena_results_bucket_name
  tags   = merge(var.tags, { Name = var.athena_results_bucket_name, Purpose = "athena-results" })
}

resource "aws_s3_bucket" "glue_scripts" {
  bucket = var.glue_scripts_bucket_name
  tags   = merge(var.tags, { Name = var.glue_scripts_bucket_name, Purpose = "glue-scripts" })
}

resource "aws_s3_bucket" "iceberg" {
  bucket = var.iceberg_bucket_name
  tags   = merge(var.tags, { Name = var.iceberg_bucket_name, Purpose = "iceberg-warehouse" })
}

# Enable versioning on all buckets
resource "aws_s3_bucket_versioning" "source" {
  bucket = aws_s3_bucket.source.id
  versioning_configuration { status = "Enabled" }
}
resource "aws_s3_bucket_versioning" "target" {
  bucket = aws_s3_bucket.target.id
  versioning_configuration { status = "Enabled" }
}
resource "aws_s3_bucket_versioning" "iceberg" {
  bucket = aws_s3_bucket.iceberg.id
  versioning_configuration { status = "Enabled" }
}

# Block public access on all buckets
resource "aws_s3_bucket_public_access_block" "source" {
  bucket                  = aws_s3_bucket.source.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_public_access_block" "target" {
  bucket                  = aws_s3_bucket.target.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket                  = aws_s3_bucket.athena_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_public_access_block" "glue_scripts" {
  bucket                  = aws_s3_bucket.glue_scripts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_public_access_block" "iceberg" {
  bucket                  = aws_s3_bucket.iceberg.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Upload Glue script to S3
resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "scripts/${var.glue_script_filename}"
  source = "${path.module}/../${var.glue_script_filename}"
  etag   = filemd5("${path.module}/../${var.glue_script_filename}")
  tags   = var.tags
}

# ---------------------------------------------------------------------------
# IAM Role for Glue
# ---------------------------------------------------------------------------
data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${var.project_name}-${var.environment}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_permissions" {
  statement {
    sid    = "S3Access"
    effect = "Allow"
    actions = ["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket"]
    resources = [
      aws_s3_bucket.source.arn,         "${aws_s3_bucket.source.arn}/*",
      aws_s3_bucket.target.arn,         "${aws_s3_bucket.target.arn}/*",
      aws_s3_bucket.athena_results.arn, "${aws_s3_bucket.athena_results.arn}/*",
      aws_s3_bucket.glue_scripts.arn,   "${aws_s3_bucket.glue_scripts.arn}/*",
      aws_s3_bucket.iceberg.arn,        "${aws_s3_bucket.iceberg.arn}/*",
    ]
  }
  statement {
    sid    = "AthenaAccess"
    effect = "Allow"
    actions = ["athena:StartQueryExecution","athena:GetQueryExecution","athena:GetQueryResults"]
    resources = ["*"]
  }
  statement {
    sid    = "GlueCatalogAccess"
    effect = "Allow"
    actions = ["glue:GetDatabase","glue:GetTable","glue:CreateTable","glue:DeleteTable",
               "glue:UpdateTable","glue:BatchCreatePartition","glue:GetPartitions"]
    resources = ["*"]
  }
  statement {
    sid    = "CloudWatchLogs"
    effect = "Allow"
    actions = ["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"]
    resources = ["arn:aws:logs:*:*:/aws-glue/*"]
  }
}

resource "aws_iam_role_policy" "glue_permissions" {
  name   = "${var.project_name}-${var.environment}-glue-policy"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_permissions.json
}

# ---------------------------------------------------------------------------
# Glue Catalog Database
# ---------------------------------------------------------------------------
resource "aws_glue_catalog_database" "this" {
  name        = var.athena_database
  description = "${var.project_name} ${var.environment} database"
}

# ---------------------------------------------------------------------------
# Glue Catalog Table (Hive-style Parquet)
# ---------------------------------------------------------------------------
resource "aws_glue_catalog_table" "parquet" {
  name          = var.athena_table
  database_name = aws_glue_catalog_database.this.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"      = "parquet"
    "parquet.compression" = "SNAPPY"
    "EXTERNAL"            = "TRUE"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.target.bucket}/${var.athena_table}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters            = { "serialization.format" = "1" }
    }

    columns {
      name = "id"
      type = "string"
    }
    columns {
      name = "name"
      type = "string"
    }
  }
}

# ---------------------------------------------------------------------------
# Glue Catalog Table (Iceberg)
# ---------------------------------------------------------------------------
resource "aws_glue_catalog_table" "iceberg" {
  name          = var.iceberg_table
  database_name = aws_glue_catalog_database.this.name
  table_type    = "EXTERNAL_TABLE"

  open_table_format_input {
    iceberg_input {
      metadata_operation = "CREATE"
      version            = "2"
    }
  }

  storage_descriptor {
    location = "s3://${aws_s3_bucket.iceberg.bucket}/${var.iceberg_table}/"
    columns {
      name = "id"
      type = "string"
    }
    columns {
      name = "name"
      type = "string"
    }
  }
}

# ---------------------------------------------------------------------------
# AWS Glue Job
# ---------------------------------------------------------------------------
resource "aws_glue_job" "this" {
  name              = var.glue_job_name
  role_arn          = aws_iam_role.glue.arn
  glue_version      = var.glue_version
  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers
  timeout           = var.job_timeout_minutes
  max_retries       = var.max_retries

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/scripts/${var.glue_script_filename}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.glue_scripts.bucket}/spark-logs/"
    "--TempDir"                          = "s3://${aws_s3_bucket.glue_scripts.bucket}/tmp/"
    "--source_s3_path"                   = "s3://${aws_s3_bucket.source.bucket}/raw/"
    "--target_s3_path"                   = "s3://${aws_s3_bucket.target.bucket}/${var.athena_table}/"
    "--athena_database"                  = var.athena_database
    "--athena_table"                     = var.athena_table
    "--athena_output_s3"                 = "s3://${aws_s3_bucket.athena_results.bucket}/results/"
    "--iceberg_table"                    = var.iceberg_table
    "--iceberg_s3_path"                  = "s3://${aws_s3_bucket.iceberg.bucket}/${var.iceberg_table}/"
    "--iceberg_warehouse"                = "s3://${aws_s3_bucket.iceberg.bucket}/warehouse/"
  }

  tags = var.tags
}

# ---------------------------------------------------------------------------
# CloudWatch Log Group
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "glue" {
  name              = "/aws-glue/jobs/${var.glue_job_name}"
  retention_in_days = 30
  tags              = var.tags
}

# ---------------------------------------------------------------------------
# DynamoDB table for Terraform state locking
# ---------------------------------------------------------------------------
resource "aws_dynamodb_table" "terraform_locks" {
  name         = "${var.project_name}-terraform-locks"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  tags = var.tags
}
