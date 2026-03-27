# Databricks notebook source
# DBTITLE 1,List files in volume
dbutils.fs.ls("/Volumes/enqurious_insurance/bronze/source_data/71bb240a-008e-4aad-bbb5-bec7756b7525_autoinsurancedata (1)")

# COMMAND ----------

/Volumes/enqurious_insurance/bronze/source_data/71bb240a-008e-4aad-bbb5-bec7756b7525_autoinsurancedata (1)/autoinsurancedata/claims_2.json

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

file_path = "/Volumes/enqurious_insurance/bronze/source_data/71bb240a-008e-4aad-bbb5-bec7756b7525_autoinsurancedata (1)/autoinsurancedata/claims_2.json"

df_raw = (
    spark.read
    .option("multiLine", "true")
    .json(file_path))

df_raw.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("enqurious_insurance.raw.claims_2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from enqurious_insurance.raw.claims_2

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

file_path = "/Volumes/enqurious_insurance/bronze/source_data/71bb240a-008e-4aad-bbb5-bec7756b7525_autoinsurancedata (1)/autoinsurancedata/Insurance 6/claims_1.json"

df_raw = (
    spark.read
    .option("multiLine", "true")
    .json(file_path))

df_raw.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("enqurious_insurance.raw.claims_1")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

base_catalog = "enqurious_insurance"
bronze_schema = "raw"

files = [
    ("cars",
     "/Volumes/enqurious_insurance/bronze/source_data/71bb240a-008e-4aad-bbb5-bec7756b7525_autoinsurancedata (1)/autoinsurancedata/Insurance 4/cars.csv"),

    ("policy",
     "/Volumes/enqurious_insurance/bronze/source_data/71bb240a-008e-4aad-bbb5-bec7756b7525_autoinsurancedata (1)/autoinsurancedata/Insurance 5/policy.csv"),

    ("customers_4",
     "/Volumes/enqurious_insurance/bronze/source_data/71bb240a-008e-4aad-bbb5-bec7756b7525_autoinsurancedata (1)/autoinsurancedata/Insurance 4/customers_4.csv"),

    ("customers_6",
     "/Volumes/enqurious_insurance/bronze/source_data/71bb240a-008e-4aad-bbb5-bec7756b7525_autoinsurancedata (1)/autoinsurancedata/Insurance 6/customers_6.csv"),

    ("customers_5",
     "/Volumes/enqurious_insurance/bronze/source_data/71bb240a-008e-4aad-bbb5-bec7756b7525_autoinsurancedata (1)/autoinsurancedata/Insurance 5/customers_5.csv"),

    ("customers_3",
     "/Volumes/enqurious_insurance/bronze/source_data/71bb240a-008e-4aad-bbb5-bec7756b7525_autoinsurancedata (1)/autoinsurancedata/Insurance 3/customers_3.csv"),

    ("customers_2",
     "/Volumes/enqurious_insurance/bronze/source_data/71bb240a-008e-4aad-bbb5-bec7756b7525_autoinsurancedata (1)/autoinsurancedata/Insurance 2/customers_2.csv"),

    ("customers_1",
     "/Volumes/enqurious_insurance/bronze/source_data/71bb240a-008e-4aad-bbb5-bec7756b7525_autoinsurancedata (1)/autoinsurancedata/Insurance 1/customers_1.csv"),

    ("sales_2",
     "/Volumes/enqurious_insurance/bronze/source_data/71bb240a-008e-4aad-bbb5-bec7756b7525_autoinsurancedata (1)/autoinsurancedata/Insurance 1/Sales_2.csv"),

    ("sales_1",
     "/Volumes/enqurious_insurance/bronze/source_data/71bb240a-008e-4aad-bbb5-bec7756b7525_autoinsurancedata (1)/autoinsurancedata/Insurance 2/sales_1.csv"),

    ("sales_4",
     "/Volumes/enqurious_insurance/bronze/source_data/71bb240a-008e-4aad-bbb5-bec7756b7525_autoinsurancedata (1)/autoinsurancedata/Insurance 3/sales_4.csv"),
    ("customers_7","/Volumes/enqurious_insurance/bronze/source_data/71bb240a-008e-4aad-bbb5-bec7756b7525_autoinsurancedata (1)/autoinsurancedata/customers_7.csv")
    
]

for table_name, path in files:

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path)
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )

    df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{base_catalog}.{bronze_schema}.{table_name}")

    print(f"Created table: {base_catalog}.{bronze_schema}.{table_name}")

# COMMAND ----------

Created table: enqurious_insurance.raw.cars
Created table: enqurious_insurance.raw.policy
Created table: enqurious_insurance.raw.customers_4
Created table: enqurious_insurance.raw.customers_6
Created table: enqurious_insurance.raw.customers_5
Created table: enqurious_insurance.raw.customers_3
Created table: enqurious_insurance.raw.customers_2
Created table: enqurious_insurance.raw.customers_1
Created table: enqurious_insurance.raw.sales_2
Created table: enqurious_insurance.raw.sales_1
Created table: enqurious_insurance.raw.sales_4

# COMMAND ----------

