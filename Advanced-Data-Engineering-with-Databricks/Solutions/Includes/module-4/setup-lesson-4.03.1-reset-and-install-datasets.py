# Databricks notebook source
# MAGIC %run ../_databricks-academy-helper $lesson="4.03"

# COMMAND ----------

# MAGIC %run ../_utility-functions

# COMMAND ----------

DA.cleanup()
DA.init()

# COMMAND ----------

def clone_bronze_dev_table():
    import time
    
    ################################
    # Cloning bronze_dev
    start = int(time.time())
    print(f"Creating bronze_dev", end="...")
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS bronze_dev
      SHALLOW CLONE {DA.source_db_name}.producer
      LOCATION '{DA.paths.user_db}/bronze_dev'
    """)
    print(f"({int(time.time())-start} seconds)")
    
def create_producer_30m_table_source():
    import time
    
    ################################
    # Cloning producer_30m table
    start = int(time.time())
    print(f"Cloning to source database producer_30m", end="...")
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {DA.source_db_name}.producer_30m
      DEEP CLONE delta.`{DA.data_source_uri}/kafka-30min`
    """) # No location for source db
    print(f"({int(time.time())-start} seconds)")

# def create_producer_30m_table():
#     import time
    
#     start = int(time.time())
#     print(f"Creating date_lookup", end="...")
    
#     spark.sql(f"""
#       CREATE TABLE IF NOT EXISTS date_lookup
#       DEEP CLONE {DA.source_db_name}.date_lookup
#       LOCATION '{DA.paths.user_db}/date_lookup'
#     """)
    
#     total = spark.read.table("date_lookup").count()
#     print(f"({int(time.time())-start} seconds / {total:,} records)")
    
None # Suppressing Output

# COMMAND ----------

# Create the source datasets
create_source_database()          # Create the source database
create_producer_table_source()    # Clone the producer table
create_date_lookup_source()
# create_user_lookup_source()
create_producer_30m_table_source()
print()

# Create the user datasets
create_date_lookup()              # Create static copy of date_lookup

# TODO - Implication from main notebook is that this should not be called
# create_user_lookup()              # Create the user-lookup table

clone_bronze_dev_table()
#create_producer_30m_table()

print()

# init_source_daily()               # Create the data factory
# DA.data_factory.load()            # Load one new day for DA.paths.source_daily

# DA.process_bronze()               # Process through the bronze table
# DA.process_heart_rate_silver()    # Process the heart_rate_silver table
# DA.process_workouts_silver()      # Process the workouts_silver table
# DA.process_completed_workouts()   # Process the completed_workouts table
# DA.process_workout_bpm()

# DA.process_users()
# build_user_bins()

# COMMAND ----------

DA.conclude_setup()

