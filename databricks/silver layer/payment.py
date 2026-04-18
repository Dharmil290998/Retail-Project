# Databricks notebook source
# MAGIC %run "/Workspace/Users/pateldharmilkumar@gmail.com/Retail-Project/databricks/silver layer/service_principle"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import (col, trim, to_date, row_number, current_timestamp, lit,concat_ws,coalesce)
from pyspark.sql.types import *

# COMMAND ----------


storage_account = "project1demo"
container_name = "project1"
table_name = "payment"

bronze_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/Sales Project/Rest API/{table_name}/*"


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DoubleType, TimestampType, LongType, StringType
schema = StructType([
    StructField("payment_id", LongType(), True),
    StructField("amount", DoubleType(), True),
    StructField("modified_date", TimestampType(), True),
    StructField("order_id", LongType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("payment_time", TimestampType(), True),
    StructField("_corrupt_record", StringType(), True)
])

# COMMAND ----------

# Read Transaction CSV from Bronze
df = spark.read \
    .format("json") \
    .option("header", "true") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .schema(schema) \
    .load(bronze_path)

display(df)

# COMMAND ----------

df.columns

# COMMAND ----------

# PK FILTER (payment_id must be valid)
df = df.filter(
    col("payment_id").isNotNull() &
    (trim(col("payment_id")) != "") &
    (col("payment_id") != "0")
)

# COMMAND ----------

# FK VALIDATION (order_id must exist)
df = df.filter(col("order_id").isNotNull())

# COMMAND ----------

# BUSINESS RULES
df = df.filter(col("amount") >= 0)

# COMMAND ----------

# STANDARDIZE VALUES 
df = df.withColumn("payment_status", upper(trim(col("payment_status")))) \
       .withColumn("payment_method", initcap(trim(col("payment_method"))))

# COMMAND ----------

# DEDUPLICATION (Keep Latest Record)
w = Window.partitionBy("payment_id").orderBy(
    col("modified_date").desc()
)

df = (
    df.withColumn("row_num", row_number().over(w))
      .filter(col("row_num") == 1)
      .drop("row_num")
)

# COMMAND ----------

# ADD DERIVED COLUMN (IMPORTANT FOR ANALYTICS)
df = df.withColumn(
    "is_success",
    when(col("payment_status") == "SUCCESS", 1).otherwise(0)
)

# COMMAND ----------

# ADD INGESTION TIMESTAMP
df = df.withColumn("ingestion_timestamp", current_timestamp())


# COMMAND ----------

# WRITE TO UNITY CATALOG
silver_table = "sales.silver.payment"

df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(silver_table)

print("Silver table created:", silver_table)
