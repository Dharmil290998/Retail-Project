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
table_name = "stores"

bronze_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/Sales Project/On-Premises/{table_name}/*"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema

# COMMAND ----------

schema = StructType([
    StructField("StoreID", IntegerType(), True),
    StructField("StoreName", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Province", StringType(), True),
    StructField("ModifiedDate", TimestampType(), True),
    StructField("_corrupt_record", StringType(), True)   
])

# COMMAND ----------


# Read Transaction CSV from Bronze
df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .schema(schema) \
    .load(bronze_path)

display(df)

# COMMAND ----------

df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Primary Key Validation

# COMMAND ----------

# PK FILTER (StoreID must be valid)
df = df.filter(
    col("StoreID").isNotNull() &
    (trim(col("StoreID")) != "") &
    (col("StoreID") != "0")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Corrupted Records

# COMMAND ----------

pk_col = "StoreID" 

df_bad = df.filter(col("_corrupt_record").isNotNull()) \
    .select(
        col(pk_col),
        col("_corrupt_record"),
        current_timestamp().alias("error_time")
    )

df_bad.write.format("delta").mode("append").saveAsTable("sales.Loginfo.store_corruptedtable")

# COMMAND ----------

df = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Quality Check

# COMMAND ----------

#  STANDARDIZE TEXT 
df = df.withColumn("StoreName", initcap(trim(col("StoreName")))) \
       .withColumn("City", initcap(trim(col("City")))) \
       .withColumn("Province", upper(trim(col("Province"))))


# COMMAND ----------

# DERIVED COLUMN 
df = df.withColumn(
    "region",
    when(col("Province").isin("ON", "QC"), "EAST")
    .when(col("Province").isin("BC", "AB"), "WEST")
    .otherwise("OTHER")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Removing Duplicates

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

# ADD INGESTION TIMESTAMP
df = df.withColumn("ingestion_timestamp", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta Table

# COMMAND ----------

# WRITE TO UNITY CATALOG
silver_table = "sales.silver.stores"

df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(silver_table)

print("Silver table created:", silver_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales.silver.stores
