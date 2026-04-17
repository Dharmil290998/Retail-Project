# Databricks notebook source
# MAGIC %run "/Workspace/Users/pateldharmilkumar@gmail.com/Retail-Project/databricks/silver layer/service_principle"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import (col, trim, to_date, row_number, current_timestamp, lit,concat_ws,coalesce)

# COMMAND ----------


storage_account = "project1demo"
container_name = "project1"
table_name = "customers"

bronze_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/Sales Project/On-Premises/{table_name}/*"


# COMMAND ----------

#from datetime import datetime
#import pytz

#toronto_tz = pytz.timezone("America/Toronto")
#today = datetime.now(toronto_tz)

#yyyy = today.strftime("%Y")
#mm = today.strftime("%m")
#dd = today.strftime("%d")

#bronze_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{table_name}/{yyyy}/{mm}/{dd}/"

# COMMAND ----------

# Read Transaction CSV from Bronze
df = spark.read.format("csv").option("header", "true").load(bronze_path) 
display(df)

# COMMAND ----------

# CAST DATATYPES
df = (
    df.withColumn("CustomerID", col("CustomerID").cast("string"))
      .withColumn("FirstName", col("FirstName").cast("string"))
      .withColumn("LastName", col("LastName").cast("string"))
      .withColumn("City", col("City").cast("string"))
      .withColumn("Email", col("Email").cast("string"))
      .withColumn("ModifiedDate", to_timestamp(col("ModifiedDate")))
)

# COMMAND ----------

# PK FILTER (CustomerID must be valid)
df = df.filter(
    col("CustomerID").isNotNull() &
    (trim(col("CustomerID")) != "") &
    (col("CustomerID") != "0")
)

# COMMAND ----------

# DEDUPLICATION (Keep Latest Record)
w = Window.partitionBy("CustomerID").orderBy(
    col("ModifiedDate").desc()
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

# WRITE TO UNITY CATALOG (IMPORTANT CHANGE)
silver_table = "sales.silver.customer"

df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(silver_table)

print("Silver table created:", silver_table)
