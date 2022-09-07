# Databricks notebook source
df = spark.read.load("/mnt/..")
df.createOrReplaceTempView("tempView")
spark.sql(f"select * from tempView")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select *,json_tuple(_unparsed_data."Weight") weight from Product 
