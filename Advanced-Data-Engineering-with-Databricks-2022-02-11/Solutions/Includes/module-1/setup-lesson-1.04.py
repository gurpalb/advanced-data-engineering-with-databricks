# Databricks notebook source
# MAGIC %run ../_databricks-academy-helper $lesson="1.04"

# COMMAND ----------

DA.paths.checkpoints = f"{DA.paths.working_dir}/_checkpoints"

DA.cleanup()
DA.init()
DA.conclude_setup()

