# Databricks notebook source
# MAGIC %run ./Config-Script

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE LAB;

# COMMAND ----------

tables_in_use = check_silver_tables_columns(TABLES_INFO)

# COMMAND ----------

bronze_to_silver(tables_in_use)        
