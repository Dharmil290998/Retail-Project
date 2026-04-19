# Databricks notebook source
stores = spark.table("sales.silver.stores")

dim_store = stores.select(
    "StoreID",
    "StoreName",
    "City",
    "Province",
    "region"
)

dim_store.write.mode("append").saveAsTable("sales.gold.dim_store")
