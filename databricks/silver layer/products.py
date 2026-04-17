# Databricks notebook source
# MAGIC %run "/Workspace/Users/pateldharmilkumar@gmail.com/Retail-Project/databricks/silver layer/service_principle"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import (col, trim, to_date, row_number, current_timestamp, lit,concat_ws,coalesce)

# COMMAND ----------


storage_account = "project1demo"
container_name = "project1"
table_name = "products"

bronze_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/Sales Project/On-Premises/{table_name}/*"


# COMMAND ----------


# Read Transaction CSV from Bronze
df = spark.read.format("csv").option("header", "true").load(bronze_path)  
display(df)

# COMMAND ----------

df.columns

# COMMAND ----------

#  CAST DATATYPES
df = (
    df.withColumn("ProductID", col("ProductID").cast("string"))
      .withColumn("ProductName", col("ProductName").cast("string"))
      .withColumn("Category", col("Category").cast("string"))
      .withColumn("Price", col("Price").cast("double"))
      .withColumn("ModifiedDate", to_timestamp(col("ModifiedDate")))
)

# COMMAND ----------


#  PK FILTER (ProductID must be valid)
df = df.filter(
    col("ProductID").isNotNull() &
    (trim(col("ProductID")) != "") &
    (col("ProductID") != "0")
)

# COMMAND ----------

# BUSINESS RULES
df = df.filter(col("Price") >= 0)

# COMMAND ----------

# STANDARDIZE TEXT 
df = df.withColumn("ProductName", initcap(trim(col("ProductName")))) \
       .withColumn("Category", initcap(trim(col("Category"))))


# COMMAND ----------

# DEDUPLICATION (Keep Latest Record)
w = Window.partitionBy("ProductID").orderBy(
    col("ModifiedDate").desc()
)

df = (
    df.withColumn("row_num", row_number().over(w))
      .filter(col("row_num") == 1)
      .drop("row_num")
)

# COMMAND ----------


# ADD DERIVED COLUMN (optional but useful 🚀)
df = df.withColumn(
    "price_category",
    when(col("Price") > 500, "HIGH")
    .when(col("Price") > 200, "MEDIUM")
    .otherwise("LOW")
)

# COMMAND ----------

# ADD INGESTION TIMESTAMP
df = df.withColumn("ingestion_timestamp", current_timestamp())

# COMMAND ----------

# WRITE TO UNITY CATALOG
silver_table = "sales.silver.products"

df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(silver_table)

print("Silver table created:", silver_table)
