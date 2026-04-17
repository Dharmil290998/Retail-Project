# Databricks notebook source
# MAGIC %run "/Workspace/Users/pateldharmilkumar@gmail.com/Retail-Project/databricks/silver layer/service_principle"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import (col, trim, to_date, row_number, current_timestamp, lit,concat_ws,coalesce)

# COMMAND ----------


storage_account = "project1demo"
container_name = "project1"
table_name = "stores"

bronze_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/Sales Project/Rest API/{table_name}/*"


# COMMAND ----------


# Read Transaction CSV from Bronze
df = spark.read.format("csv").option("header", "true").load(bronze_path)  
display(df)

# COMMAND ----------

df.columns

# COMMAND ----------

#  CAST DATATYPES
df = (
    df.withColumn("StoreID", col("StoreID").cast("string"))
      .withColumn("StoreName", col("StoreName").cast("string"))
      .withColumn("City", col("City").cast("string"))
      .withColumn("Province", col("Province").cast("string"))
      .withColumn("ModifiedDate", to_timestamp(col("ModifiedDate")))
)

# COMMAND ----------

# PK FILTER (StoreID must be valid)
df = df.filter(
    col("StoreID").isNotNull() &
    (trim(col("StoreID")) != "") &
    (col("StoreID") != "0")
)


# COMMAND ----------

#  STANDARDIZE TEXT 
df = df.withColumn("StoreName", initcap(trim(col("StoreName")))) \
       .withColumn("City", initcap(trim(col("City")))) \
       .withColumn("Province", upper(trim(col("Province"))))


# COMMAND ----------

# DEDUPLICATION (Keep Latest Record)
w = Window.partitionBy("StoreID").orderBy(
    col("ModifiedDate").desc()
)

df = (
    df.withColumn("row_num", row_number().over(w))
      .filter(col("row_num") == 1)
      .drop("row_num")
)

# COMMAND ----------

# DERIVED COLUMN 
df = df.withColumn(
    "region",
    when(col("Province").isin("ON", "QC"), "EAST")
    .when(col("Province").isin("BC", "AB"), "WEST")
    .otherwise("OTHER")
)

# COMMAND ----------

# ADD INGESTION TIMESTAMP
df = df.withColumn("ingestion_timestamp", current_timestamp())

# COMMAND ----------

# WRITE TO UNITY CATALOG
silver_table = "sales.silver.stores"

df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(silver_table)

print("Silver table created:", silver_table)
