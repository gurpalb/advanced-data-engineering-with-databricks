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

DA.paths.streaming_logs_json = f"{DA.paths.working_dir}/streaming_logs"
DA.paths.streaming_logs_delta = f"{DA.paths.working_dir}/streaming_logs_delta"

# COMMAND ----------

DA.conclude_setup()

