# Databricks notebook source
# MAGIC %run "/Workspace/Users/pateldharmilkumar@gmail.com/Retail-Project/databricks/silver layer/service_principle"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import (col, trim, to_date, row_number, current_timestamp, lit,concat_ws,coalesce)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

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

# MAGIC %md
# MAGIC ### Schema

# COMMAND ----------

# 🔹 Step 1: Define schema
schema = StructType([
    StructField("CustomerID", IntegerType(), True),
    StructField("FirstName", StringType(), True),
    StructField("LastName", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("ModifiedDate", TimestampType(), True),
    StructField("_corrupt_record", StringType(), True)
])

# COMMAND ----------

df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .schema(schema) \
    .load(bronze_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Primary Key Validation

# COMMAND ----------

# PK FILTER (CustomerID must be valid)
df = df.filter(
    col("CustomerID").isNotNull() &
    (trim(col("CustomerID")) != "") &
    (col("CustomerID") != "0")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Corruped Records

# COMMAND ----------

pk_col = "CustomerID" 

df_bad = df.filter(col("_corrupt_record").isNotNull()) \
    .select(
        col(pk_col),
        col("_corrupt_record"),
        current_timestamp().alias("error_time")
    )

df_bad.write.format("delta").mode("append").saveAsTable("sales.Loginfo.customers_corruptedtable")

# COMMAND ----------

df = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Quality Check

# COMMAND ----------

df = df.withColumn(
    "FullName",
    concat_ws(" ", "FirstName", "LastName")
)

# COMMAND ----------

df = df.drop("FirstName", "LastName")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Removing Duplicates

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

# MAGIC %md
# MAGIC ### Delta Table

# COMMAND ----------

# save table
silver_table = "sales.silver.customer"

df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(silver_table)

print("Silver table created:", silver_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales.silver.customer

# COMMAND ----------


