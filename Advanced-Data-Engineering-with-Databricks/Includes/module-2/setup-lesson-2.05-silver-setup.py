# Databricks notebook source
# MAGIC %run ../_databricks-academy-helper $lesson="2.05"

# COMMAND ----------

# MAGIC %run ../_utility-functions

# COMMAND ----------

DA.cleanup()
DA.init()

# COMMAND ----------

# Create the source datasets
create_source_database()          # Create the source database
create_producer_table_source()    # Clone the producer table
create_date_lookup_source()
print()

# Create the user datasets
create_date_lookup()              # Create static copy of date_lookup
print()

init_source_daily()               # Create the data factory
DA.data_factory.load()            # Load one new day for DA.paths.source_daily

DA.process_bronze()               # Process through the bronze table

# COMMAND ----------

DA.conclude_setup()

