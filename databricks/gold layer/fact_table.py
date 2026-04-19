# Databricks notebook source
from pyspark.sql.functions import *

orderdetails = spark.table("sales.silver.orderdetails")
orders       = spark.table("sales.silver.orders")
payment      = spark.table("sales.silver.payment")
shipping     = spark.table("sales.silver.shipping")

# COMMAND ----------

fact_sales = (
    orderdetails.alias("od")
    .join(orders.alias("o"), col("od.OrderID") == col("o.OrderID"))
    .join(payment.alias("p"), col("od.OrderID") == col("p.order_id"), "left")
    .join(shipping.alias("s"), col("od.OrderID") == col("s.order_id"), "left")
    .select(
        col("od.OrderDetailID"),
        col("od.OrderID"),
        col("od.ProductID"),
        col("o.CustomerID"),
        col("o.StoreID"),
        col("o.OrderDate"),

        col("od.Quantity"),
        col("od.TotalAmount").alias("SalesAmount"),

        col("p.amount").alias("PaymentAmount"),
        col("p.is_success"),
        col("p.payment_id"),
        col("s.is_delivered"),
        col("s.shipment_id"),
        col("s.delivery_days"),
        current_timestamp().alias("created_at")
    )
)

# COMMAND ----------

display(fact_sales)

# COMMAND ----------

fact_sales.columns

# COMMAND ----------

fact_sales.write.format("delta") \
    .mode("append") \
    .saveAsTable("sales.gold.fact_sales")
