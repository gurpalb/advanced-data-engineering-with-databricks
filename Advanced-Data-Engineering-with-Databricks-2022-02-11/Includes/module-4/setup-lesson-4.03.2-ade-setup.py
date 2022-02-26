# Databricks notebook source
# MAGIC %run ../_databricks-academy-helper $lesson="4.03"

# COMMAND ----------

# MAGIC %run ../_utility-functions

# COMMAND ----------

# Do not cleanup!
# Intentionally keeping State from the previous notebook
# DA.cleanup() 
DA.init()

# Doesn't actually create but initialize variables
create_source_database() 

DA.paths.producer_30m = f"{DA.paths.working_dir}/producer_30m"
DA.paths.streaming_logs_json = f"{DA.paths.working_dir}/streaming_logs"
DA.paths.streaming_logs_delta = f"{DA.paths.working_dir}/streaming_logs_delta"

dbutils.fs.mkdirs(DA.paths.producer_30m)

# COMMAND ----------

DA.conclude_setup()

