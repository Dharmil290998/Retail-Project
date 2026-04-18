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
table_name = "orders"

bronze_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/Sales Project/On-Premises/{table_name}/*"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema

# COMMAND ----------

schema = StructType([
    StructField("OrderID", IntegerType(), True),
    StructField("CustomerID", IntegerType(), True),
    StructField("StoreID", IntegerType(), True),
    StructField("OrderDate", TimestampType(), True),
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

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Primary Key Validation

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

from pyspark.sql.functions import col, to_date, to_timestamp

df = (df.withColumn("OrderDate", to_date(col("OrderDate"), "yyyy-MM-dd"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Corruped Records

# COMMAND ----------

pk_col = "OrderID" 

df_bad = df.filter(col("_corrupt_record").isNotNull()) \
    .select(
        col(pk_col),
        col("_corrupt_record"),
        current_timestamp().alias("error_time")
    )

df_bad.write.format("delta").mode("append").saveAsTable("sales.Loginfo.orders_corruptedtable")

# COMMAND ----------

df = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Quality Check

# COMMAND ----------

# ADD DERIVED COLUMN
df = df.withColumn("OrderYear", year(col("OrderDate"))) \
       .withColumn("OrderMonth", month(col("OrderDate")))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Removing Duplicates

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

# ADD INGESTION TIMESTAMP
df = df.withColumn("ingestion_timestamp", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Delta Table

# COMMAND ----------

# WRITE TO UNITY CATALOG
silver_table = "sales.silver.orders"

df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(silver_table)

print("Silver table created:", silver_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales.silver.orders

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table sales.silver.orders

# COMMAND ----------


