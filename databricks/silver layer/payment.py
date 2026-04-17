# Databricks notebook source
# MAGIC %run "/Workspace/Users/pateldharmilkumar@gmail.com/Retail-Project/databricks/silver layer/service_principle"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import (col, trim, to_date, row_number, current_timestamp, lit,concat_ws,coalesce)

# COMMAND ----------


storage_account = "project1demo"
container_name = "project1"
table_name = "payment"

bronze_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/Sales Project/On-Premises/{table_name}/*"


# COMMAND ----------


# Read Transaction CSV from Bronze
df = spark.read.format("csv").option("header", "true").load(bronze_path)  
display(df)

# COMMAND ----------

df.columns

# COMMAND ----------


# CAST DATATYPES
df = (
    df.withColumn("payment_id", col("payment_id").cast("string"))
      .withColumn("order_id", col("order_id").cast("string"))
      .withColumn("payment_method", col("payment_method").cast("string"))
      .withColumn("payment_status", col("payment_status").cast("string"))
      .withColumn("amount", col("amount").cast("double"))
      .withColumn("payment_time", to_timestamp(col("payment_time")))
      .withColumn("modified_date", to_timestamp(col("modified_date")))
)


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
