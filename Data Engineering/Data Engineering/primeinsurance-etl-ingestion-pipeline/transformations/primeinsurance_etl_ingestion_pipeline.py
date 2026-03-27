import dlt
from pyspark.sql import functions as F

VOLUME_PATH = "/Volumes/primeinsurance/bronze/raw_files/autoinsurancedata"
CHECKPOINT_BASE = "/Volumes/primeinsurance/bronze/raw_files/_checkpoints"

# Point to entire autoinsurancedata folder — Auto Loader
# will find customers_*.csv at ALL levels including root
@dlt.table(
    name="customers",
    comment="Raw customer records from all 6 regional systems — 7 CSV files merged",
    table_properties={"quality": "bronze"}
)
def bronze_customers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mergeSchema", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/customers_schema")
        .option("pathGlobFilter", "customers_*.csv")
        .load(f"{VOLUME_PATH}")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_load_timestamp", F.current_timestamp())
    )

# pathGlobFilter picks up claims_*.json at ALL levels
# including claims_2.json at root
@dlt.table(
    name="claims",
    comment="Raw claims records from 2 regional JSON files",
    table_properties={"quality": "bronze"}
)
def bronze_claims():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("inferSchema", "false")
        .option("mergeSchema", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/claims_schema")
        .option("pathGlobFilter", "claims_*.json")
        .load(f"{VOLUME_PATH}")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_load_timestamp", F.current_timestamp())
    )

@dlt.table(
    name="policy",
    comment="Raw policy records from Insurance 5",
    table_properties={"quality": "bronze"}
)
def bronze_policy():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/policy_schema")
        .option("pathGlobFilter", "policy.csv")
        .load(f"{VOLUME_PATH}")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_load_timestamp", F.current_timestamp())
    )

# pathGlobFilter handles both Sales_2.csv and sales_1.csv
@dlt.table(
    name="sales",
    comment="Raw sales records from 3 regional CSV files",
    table_properties={"quality": "bronze"}
)
def bronze_sales():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mergeSchema", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/sales_schema")
        .option("pathGlobFilter", "*[Ss]ales*.csv")
        .load(f"{VOLUME_PATH}")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_load_timestamp", F.current_timestamp())
    )

@dlt.table(
    name="cars",
    comment="Raw vehicle catalogue from Insurance 4 — 2500 records",
    table_properties={"quality": "bronze"}
)
def bronze_cars():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/cars_schema")
        .option("pathGlobFilter", "cars.csv")
        .load(f"{VOLUME_PATH}")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_load_timestamp", F.current_timestamp())
    )