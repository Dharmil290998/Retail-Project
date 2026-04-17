# Databricks notebook source
# MAGIC %run "/Workspace/Users/pateldharmilkumar@gmail.com/Retail-Project/databricks/silver layer/service_principle"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import (col, trim, to_date, row_number, current_timestamp, lit,concat_ws,coalesce)

# COMMAND ----------


storage_account = "project1demo"
container_name = "project1demo"
table_name = "shipping"

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
    df.withColumn("shipment_id", col("shipment_id").cast("string"))
      .withColumn("order_id", col("order_id").cast("string"))
      .withColumn("shipment_status", col("shipment_status").cast("string"))
      .withColumn("shipment_date", to_date(col("shipment_date")))
      .withColumn("delivery_date", to_date(col("delivery_date")))
      .withColumn("carrier", col("carrier").cast("string"))
      .withColumn("modified_date", to_timestamp(col("modified_date")))
)

# COMMAND ----------

# PK FILTER (shipment_id must be valid)
df = df.filter(
    col("shipment_id").isNotNull() &
    (trim(col("shipment_id")) != "") &
    (col("shipment_id") != "0")
)


# COMMAND ----------

# FK VALIDATION
df = df.filter(col("order_id").isNotNull())

# COMMAND ----------

# STANDARDIZE TEXT
df = df.withColumn("shipment_status", upper(trim(col("shipment_status")))) \
       .withColumn("carrier", initcap(trim(col("carrier"))))


# COMMAND ----------

# DEDUPLICATION (Keep Latest Record)
w = Window.partitionBy("shipment_id").orderBy(
    col("modified_date").desc()
)

df = (
    df.withColumn("row_num", row_number().over(w))
      .filter(col("row_num") == 1)
      .drop("row_num")
)

# COMMAND ----------

# DERIVED COLUMN 

# Delivery Time (in days)
df = df.withColumn(
    "delivery_days",
    datediff(col("delivery_date"), col("shipment_date"))
)

# Delivery Status Flag
df = df.withColumn(
    "is_delivered",
    when(col("shipment_status") == "DELIVERED", 1).otherwise(0)
)

# COMMAND ----------

# ADD INGESTION TIMESTAMP
df = df.withColumn("ingestion_timestamp", current_timestamp())


# COMMAND ----------

#  WRITE TO UNITY CATALOG
silver_table = "sales.silver.shipping"

df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(silver_table)

print("Silver table created:", silver_table)
