# Databricks notebook source
# MAGIC %md
# MAGIC ### SCD Type 1 on Product Table

# COMMAND ----------

from pyspark.sql.functions import *
#  Source data
df = spark.read.format("delta").table("sales.silver.products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hash Column at Source 

# COMMAND ----------

#  Add HASH column 
df_hash = df.withColumn(
    "src_hash",
    crc32(concat_ws("||",
        col("ProductID").cast("string"),
        col("ProductName"),
        col("Category"),
        col("Price").cast("string"),
        col("price_category"),
        col("ModifiedDate").cast("string")
    ))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Target Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF Not EXISTS sales.gold.dim_product
# MAGIC (
# MAGIC ProductID integer,
# MAGIC ProductName string,
# MAGIC Category string,
# MAGIC Price double,
# MAGIC price_category string,
# MAGIC ModifiedDate timestamp,
# MAGIC HASHVALUE string,
# MAGIC CREATEDDATE timestamp,
# MAGIC CREATEDBY string,
# MAGIC UPDATEDDATE timestamp,
# MAGIC UPDATEDBY string
# MAGIC )

# COMMAND ----------

# Target table
from delta.tables import DeltaTable
table_name = "sales.gold.dim_product"
delta_tgt = DeltaTable.forName(spark, table_name)


# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD 1 Merge Logic

# COMMAND ----------


# MERGE (SCD TYPE 1)
(
    delta_tgt.alias("tgt")
    .merge(
        df_hash.alias("src"),
        "tgt.ProductID = src.ProductID"
    )
    .whenMatchedUpdate(
        condition = "tgt.HASHVALUE != src.src_hash",
        set = {
            "ProductID": "src.ProductID",
            "ProductName": "src.ProductName",
            "Category": "src.Category",
            "Price": "src.Price",
            "ModifiedDate": "src.ModifiedDate",
            "price_category": "src.price_category",
            "HASHVALUE": "src.src_hash",
            "UPDATEDDATE": current_timestamp(),
            "UPDATEDBY": lit("databricks-updated")
        }
    )
    .whenNotMatchedInsert(
        values = {
            "ProductID": "src.ProductID",
            "ProductName": "src.ProductName",
            "Category": "src.Category",
            "Price": "src.Price",
            "ModifiedDate": "src.ModifiedDate",
            "price_category": "src.price_category",
            "HASHVALUE": "src.src_hash",
            "CREATEDDATE": current_timestamp(),
            "CREATEDBY": lit("databricks"),
            "UPDATEDDATE": current_timestamp(),
            "UPDATEDBY": lit("databricks")
        }
    )
    .execute()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales.gold.dim_product
