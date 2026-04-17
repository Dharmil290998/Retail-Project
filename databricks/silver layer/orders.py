# Databricks notebook source
# MAGIC %run "/Workspace/Users/pateldharmilkumar@gmail.com/Retail-Project/databricks/silver layer/service_principle"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import (col, trim, to_date, row_number, current_timestamp, lit,concat_ws,coalesce)

# COMMAND ----------


storage_account = "project1demo"
container_name = "project1demo"
table_name = "orders"

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
    df.withColumn("OrderID", col("OrderID").cast("string"))
      .withColumn("CustomerID", col("CustomerID").cast("string"))
      .withColumn("StoreID", col("StoreID").cast("string"))
      .withColumn("OrderDate", to_date(col("OrderDate")))
      .withColumn("ModifiedDate", to_timestamp(col("ModifiedDate")))
)

# COMMAND ----------

# PK FILTER (OrderID must be valid)
df = df.filter(
    col("OrderID").isNotNull() &
    (trim(col("OrderID")) != "") &
    (col("OrderID") != "0")
)

# COMMAND ----------

# FK VALIDATION (optional but good practice)
df = df.filter(
    col("CustomerID").isNotNull() &
    col("StoreID").isNotNull()
)

# COMMAND ----------

# DEDUPLICATION (Keep Latest Record)
w = Window.partitionBy("OrderID").orderBy(
    col("ModifiedDate").desc()
)

df = (
    df.withColumn("row_num", row_number().over(w))
      .filter(col("row_num") == 1)
      .drop("row_num")
)

# COMMAND ----------

# ADD DERIVED COLUMN (VERY IMPORTANT 🚀)
df = df.withColumn("OrderYear", year(col("OrderDate"))) \
       .withColumn("OrderMonth", month(col("OrderDate")))


# COMMAND ----------

# ADD INGESTION TIMESTAMP
df = df.withColumn("ingestion_timestamp", current_timestamp())

# COMMAND ----------

# WRITE TO UNITY CATALOG
silver_table = "sales.silver.orders"

df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(silver_table)

print("Silver table created:", silver_table)
