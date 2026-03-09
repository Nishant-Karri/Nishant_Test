"""
AWS Glue Job: Read JSON from S3, select id and name columns, convert to Parquet,
write to different S3 location. Runs MSCK REPAIR TABLE on the Hive-style Parquet
table, then creates (or replaces) an Apache Iceberg table from the same data.

Usage:
  Configure job parameters via Glue job arguments:
    --source_s3_path      s3://your-source-bucket/path/to/input.json (or prefix/)
    --target_s3_path      s3://your-target-bucket/path/to/output/
    --athena_database     e.g. my_database
    --athena_table        e.g. my_parquet_table
    --athena_output_s3    e.g. s3://my-bucket/athena-query-results/
    --iceberg_table       e.g. my_iceberg_table
    --iceberg_s3_path     e.g. s3://my-bucket/iceberg/my_iceberg_table/
    --iceberg_warehouse   e.g. s3://my-bucket/iceberg/warehouse/

  Run via AWS Glue console, CLI, or Step Functions.

  Note: Enable the Glue job parameter --datalake-formats=iceberg and set
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
        in Glue job additional configs.
"""

import sys
import time
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "source_s3_path",     # e.g. s3://my-bucket/raw/events/data.json
        "target_s3_path",     # e.g. s3://my-bucket/processed/events/
        "athena_database",    # e.g. my_database
        "athena_table",       # e.g. my_parquet_table
        "athena_output_s3",   # e.g. s3://my-bucket/athena-query-results/
        "iceberg_table",      # e.g. my_iceberg_table
        "iceberg_s3_path",    # e.g. s3://my-bucket/iceberg/my_iceberg_table/
        "iceberg_warehouse",  # e.g. s3://my-bucket/iceberg/warehouse/
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

source_path     = args["source_s3_path"]
target_path     = args["target_s3_path"]
athena_database = args["athena_database"]
athena_table    = args["athena_table"]
athena_output_s3 = args["athena_output_s3"]
iceberg_table   = args["iceberg_table"]
iceberg_s3_path = args["iceberg_s3_path"]
iceberg_warehouse = args["iceberg_warehouse"]

# ---------------------------------------------------------------------------
# Configure Spark for Iceberg + Glue Catalog
# ---------------------------------------------------------------------------
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", iceberg_warehouse)
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.extensions",
               "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

# ---------------------------------------------------------------------------
# Read JSON from S3
# ---------------------------------------------------------------------------
print(f"Reading JSON from: {source_path}")

dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [source_path], "recurse": True},
    format="json",
    format_options={"multiLine": "true"},
)

print(f"Record count: {dynamic_frame.count()}")
print("Schema:")
dynamic_frame.printSchema()

# ---------------------------------------------------------------------------
# Select id and name columns
# ---------------------------------------------------------------------------
df = dynamic_frame.toDF()
df_selected = df.select(
    col("id"),
    col("name"),
)

print("Selected columns: id, name")
print(f"Row count after select: {df_selected.count()}")
df_selected.printSchema()

# Convert back to DynamicFrame for Glue writer
dynamic_frame_selected = DynamicFrame.fromDF(df_selected, glueContext, "dynamic_frame_selected")

# ---------------------------------------------------------------------------
# (Optional) Resolve choice / clean up ambiguous types
# ---------------------------------------------------------------------------
# Uncomment and adapt if your JSON has mixed-type columns:
# dynamic_frame_selected = ResolveChoice.apply(
#     dynamic_frame_selected,
#     specs=[("id", "cast:string"), ("name", "cast:string")],
# )

# ---------------------------------------------------------------------------
# Write as Parquet to target S3 location
# ---------------------------------------------------------------------------
print(f"Writing Parquet to: {target_path}")

glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame_selected,
    connection_type="s3",
    connection_options={"path": target_path},
    format="parquet",
    format_options={
        "compression": "snappy",        # snappy | gzip | none
        "useGlueParquetWriter": "true", # optimised Glue writer
    },
)

# ---------------------------------------------------------------------------
# MSCK REPAIR TABLE — refresh Athena partition metadata
# ---------------------------------------------------------------------------
print(f"Running MSCK REPAIR TABLE on {athena_database}.{athena_table}")

athena_client = boto3.client("athena")

response = athena_client.start_query_execution(
    QueryString=f"MSCK REPAIR TABLE `{athena_database}`.`{athena_table}`",
    QueryExecutionContext={"Database": athena_database},
    ResultConfiguration={"OutputLocation": athena_output_s3},
)

query_execution_id = response["QueryExecutionId"]
print(f"Athena query execution ID: {query_execution_id}")

# Poll until the query finishes
terminal_states = {"SUCCEEDED", "FAILED", "CANCELLED"}
while True:
    status_response = athena_client.get_query_execution(
        QueryExecutionId=query_execution_id
    )
    state = status_response["QueryExecution"]["Status"]["State"]
    print(f"Athena query state: {state}")
    if state in terminal_states:
        break
    time.sleep(5)

if state != "SUCCEEDED":
    reason = status_response["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
    raise RuntimeError(f"MSCK REPAIR TABLE {state}: {reason}")

print("MSCK REPAIR TABLE complete.")

# ---------------------------------------------------------------------------
# Create Apache Iceberg Table (after MSCK REPAIR)
# ---------------------------------------------------------------------------
print(f"Creating Iceberg table: glue_catalog.{athena_database}.{iceberg_table}")

# Drop existing Iceberg table if it exists (full refresh pattern)
spark.sql(f"DROP TABLE IF EXISTS glue_catalog.`{athena_database}`.`{iceberg_table}`")

# Create the Iceberg table using Spark SQL with USING iceberg
spark.sql(f"""
    CREATE TABLE glue_catalog.`{athena_database}`.`{iceberg_table}` (
        id     STRING,
        name   STRING
    )
    USING iceberg
    LOCATION '{iceberg_s3_path}'
    TBLPROPERTIES (
        'table_type'                   = 'ICEBERG',
        'format'                       = 'parquet',
        'write.parquet.compression-codec' = 'snappy',
        'write.metadata.delete-after-commit.enabled' = 'true',
        'write.metadata.previous-versions-max' = '10'
    )
""")

print(f"Iceberg table created: glue_catalog.{athena_database}.{iceberg_table}")

# Insert data from the selected DataFrame into the Iceberg table
print("Inserting data into Iceberg table...")
df_selected.writeTo(f"glue_catalog.`{athena_database}`.`{iceberg_table}`").append()

iceberg_count = spark.table(f"glue_catalog.`{athena_database}`.`{iceberg_table}`").count()
print(f"Iceberg table row count: {iceberg_count}")
print(f"Iceberg table location: {iceberg_s3_path}")
print("Iceberg table creation complete.")

print("Job complete.")
job.commit()
