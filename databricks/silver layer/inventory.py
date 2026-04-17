# Databricks notebook source
# MAGIC %run "/Workspace/Users/pateldharmilkumar@gmail.com/Retail-Project/databricks/silver layer/service_principle"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import (col, trim, to_date, row_number, current_timestamp, lit,concat_ws,coalesce)

# COMMAND ----------


storage_account = "project1demo"
container_name = "project1"
table_name = "inventory"

bronze_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/Sales Project/Rest API/{table_name}/*"


# COMMAND ----------


# Read Transaction CSV from Bronze
df = spark.read.format("csv").option("header", "true").load(bronze_path)  
display(df)

# COMMAND ----------

df.columns

# COMMAND ----------

# CAST DATATYPES
df = (
    df.withColumn("inventory_id", col("inventory_id").cast("string"))
      .withColumn("product_id", col("product_id").cast("string"))
      .withColumn("store_id", col("store_id").cast("string"))
      .withColumn("stock_quantity", col("stock_quantity").cast("int"))
      .withColumn("last_restock_date", to_date(col("last_restock_date")))
      .withColumn("modified_date", to_timestamp(col("modified_date")))
)

# COMMAND ----------

# PK FILTER (inventory_id must be valid)
df = df.filter(
    col("inventory_id").isNotNull() &
    (trim(col("inventory_id")) != "") &
    (col("inventory_id") != "0")
)

# COMMAND ----------

# OPTIONAL BUSINESS RULE (remove negative stock)
df = df.filter(col("stock_quantity") >= 0)


# COMMAND ----------

# DEDUPLICATION (Keep Latest Record)
w = Window.partitionBy("inventory_id").orderBy(
    col("modified_date").desc()
)

df = (
    df.withColumn("row_num", row_number().over(w))
      .filter(col("row_num") == 1)
      .drop("row_num")
)

# COMMAND ----------

# ADD INGESTION TIMESTAMP
df = df.withColumn("ingestion_timestamp", current_timestamp())


# COMMAND ----------

# WRITE TO UNITY CATALOG
silver_table = "sales.silver.inventory"

df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(silver_table)

print("Silver table created:", silver_table)
