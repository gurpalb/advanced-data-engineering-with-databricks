# Databricks notebook source
# MAGIC %run ./_databricks-academy-helper $lesson="3.04"

# COMMAND ----------

# MAGIC %run ./_utility-functions

# COMMAND ----------

DA.cleanup()
DA.init()

DA.create_bronze_table()
print()

DA.conclude_setup()

