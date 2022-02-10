# Databricks notebook source
# MAGIC %run ./_databricks-academy-helper $lesson="reset"

# COMMAND ----------

# MAGIC %run ./_utility-functions

# COMMAND ----------

DA.init()

# COMMAND ----------

rows = spark.sql(f"show databases").collect()
for row in rows:
    db_name = row[0]
    if db_name.startswith(DA.db_name_prefix):
        print(db_name)
        spark.sql(f"DROP DATABASE {db_name} CASCADE")

# COMMAND ----------

if DA.paths.exists(DA.working_dir_prefix):
    print(DA.working_dir_prefix)
    dbutils.fs.rm(DA.working_dir_prefix, True)

# COMMAND ----------

# Recreate the "source" database to facilitate faster test execution and prevent multiple lessons from creating.
create_source_database()          # Create the source database
create_producer_table_source()    # Clone the producer table
create_date_lookup_source()
create_user_lookup_source()

