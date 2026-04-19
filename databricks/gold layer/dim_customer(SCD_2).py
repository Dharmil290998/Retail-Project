# Databricks notebook source
# MAGIC %md
# MAGIC ### SCD Type 2 on Customer Table

# COMMAND ----------

from pyspark.sql.functions import *

#  Source
df = spark.read.format("delta").table("sales.silver.customer")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Hash Column at Source

# COMMAND ----------

#  Add HASH
df_hash = df.withColumn("src_hash", crc32(concat("CustomerID","FullName","City","Email")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Target Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS sales.gold.dim_customer 
# MAGIC (
# MAGIC     CustomerID STRING,
# MAGIC     FullName STRING,
# MAGIC     City STRING,
# MAGIC     Email STRING,
# MAGIC     HASHVALUE BIGINT,
# MAGIC     CREATEDDATE TIMESTAMP,
# MAGIC     CREATEDBY STRING,
# MAGIC     UPDATEDDATE TIMESTAMP,
# MAGIC     UPDATEDBY STRING,
# MAGIC     IS_ACTIVE INT
# MAGIC )

# COMMAND ----------

# Target table
from delta.tables import DeltaTable
table_name = "sales.gold.dim_customer"
df_tgt = DeltaTable.forName(spark, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detect New or Changed Records

# COMMAND ----------

df_new = (
    df_hash.alias("src")
    .join(
        df_tgt.toDF().alias("tgt"),
        (col("src.CustomerID") == col("tgt.CustomerID")) &
        (col("src.src_hash") == col("tgt.HASHVALUE")),
        "left_anti"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Identify Changed Records

# COMMAND ----------

LatestRecord = (
    df_new.alias("new")
    .join(
        df_tgt.toDF().alias("old"),
        col("new.CustomerID") == col("old.CustomerID"),
        "inner"
    )
    .where(
        (col("new.src_hash") != col("old.HASHVALUE")) &
        (col("old.IS_ACTIVE") == 1)
    )
    .select(
        col("new.CustomerID").alias("MergeKey"),
        col("new.*")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare New Records

# COMMAND ----------

from pyspark.sql.functions import lit

LatestRecord1 = (
    df_new
    .select(
        lit(None).alias("MergeKey"),
        "*"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combine Changed and New Records

# COMMAND ----------

updates = LatestRecord.union(LatestRecord1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply SCD Type-2 Using Delta MERGE

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, to_timestamp

(
    df_tgt.alias("tgt")
    .merge(updates.alias("src"),"tgt.CustomerID = src.MergeKey AND IS_ACTIVE = 1")
    .whenMatchedUpdate(
        condition="tgt.IS_ACTIVE = 1 AND tgt.HASHVALUE != src.src_hash",
        set={
            "UPDATEDDATE": current_timestamp(),
            "IS_ACTIVE": lit(0),
            "UPDATEDBY": lit("Databricks-Updated")
        }
    )
    .whenNotMatchedInsert(
        values={
            "CustomerID": "src.CustomerID",
            "CITY": "src.CITY",
            "FullName": "src.FullName",
            "Email": "src.Email",
            "HASHVALUE": "src.src_hash",
            "UPDATEDDATE": to_timestamp(lit("9999-01-01 00:00:00")),
            "UPDATEDBY": lit("databricks"),
            "CREATEDDATE": current_timestamp(),
            "CREATEDBY": lit("databricks"),
            "IS_ACTIVE": lit(1)
        }
    )
    .execute()
)
