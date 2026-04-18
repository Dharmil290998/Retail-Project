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
table_name = "shipping"

bronze_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/Sales Project/Rest API/{table_name}/*"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema

# COMMAND ----------

schema = StructType([
    StructField("shipment_id", LongType(), True),
    StructField("carrier", StringType(), True),
    StructField("delivery_date", DateType(), True),
    StructField("modified_date", TimestampType(), True),
    StructField("order_id", LongType(), True),
    StructField("shipment_date", DateType(), True),
    StructField("shipment_status", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])

# COMMAND ----------

# Read Transaction CSV from Bronze
df = spark.read \
    .format("json") \
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

# MAGIC %md
# MAGIC ### Handling Corrupted Records

# COMMAND ----------

pk_col = "shipment_id" 

df_bad = df.filter(col("_corrupt_record").isNotNull()) \
    .select(
        col(pk_col),
        col("_corrupt_record"),
        current_timestamp().alias("error_time")
    )

df_bad.write.format("delta").mode("append").saveAsTable("sales.Loginfo.shipping_corruptedtable")

# COMMAND ----------

df = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Quality Check

# COMMAND ----------

# STANDARDIZE TEXT
df = df.withColumn("shipment_status", upper(trim(col("shipment_status")))) \
       .withColumn("carrier", initcap(trim(col("carrier"))))


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

from pyspark.sql.functions import datediff

df = df.withColumn(
    "delivery_days",
    when(
        col("delivery_date").isNotNull(),
        datediff(col("delivery_date"), col("shipment_date"))
    )
)

# COMMAND ----------

df = df.withColumn(
    "business_status",
    when(col("shipment_status") == "DELIVERED", "Order completed")
    .when(col("shipment_status") == "IN_TRANSIT", "Moving to destination")
    .when(col("shipment_status") == "OUT_FOR_DELIVERY", "Last mile")
    .when(col("shipment_status") == "PENDING", "Not shipped yet")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Removing Duplicates

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

# ADD INGESTION TIMESTAMP
df = df.withColumn("ingestion_timestamp", current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta Table

# COMMAND ----------

#  WRITE TO UNITY CATALOG
silver_table = "sales.silver.shipping"

df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(silver_table)

print("Silver table created:", silver_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales.silver.shipping

# COMMAND ----------


