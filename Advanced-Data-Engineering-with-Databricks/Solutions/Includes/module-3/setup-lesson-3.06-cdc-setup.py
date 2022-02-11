# Databricks notebook source
# MAGIC %run ../_databricks-academy-helper $lesson="3.06"

# COMMAND ----------

# MAGIC %run ../_utility-functions

# COMMAND ----------

def create_cdc_raw_source():
    import time
    
    start = int(time.time())
    print(f"Cloning to source database cdc_raw", end="...")
    
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {DA.source_db_name}.cdc_raw
                  DEEP CLONE delta.`{DA.data_source_uri}/pii/raw`""")
                  
    print(f"({int(time.time())-start} seconds)")


# COMMAND ----------

DA.paths.cdc_stream = f"{DA.paths.working_dir}/streams/cdc"
DA.paths.silver = f"{DA.paths.working_dir}/silver"

class CdcDataFactory:
    def __init__(self, reset=True, max_batch=3):
        self.userdir = DA.paths.cdc_stream
        self.batch = 1
        self.max_batch = max_batch
        if reset == True:
            dbutils.fs.rm(self.userdir, True)
            
    def load(self, continuous=False, silent=False):
        import time
        from pyspark.sql import functions as F
        
        start = int(time.time())
        rawDF = spark.read.table(f"{DA.source_db_name}.cdc_raw")
        
        if self.batch > self.max_batch:
            print("Data source exhausted\n")
            
        elif continuous == True:
            print("Loading all streaming data", end="...")
            while self.batch <= max_batch:
                (rawDF.filter(F.col("batch") == self.batch)
                    .select('mrn','dob','sex','gender','first_name','last_name','street_address','zip','city','state','updated')
                    .write
                    .mode("append")
                    .format("json")
                    .save(self.userdir))
                self.batch += 1
        else:
            if not silent: print(f"Loading batch #{self.batch} to cdc stream", end="...")
            (rawDF.filter(F.col("batch") == self.batch)
                .select('mrn','dob','sex','gender','first_name','last_name','street_address','zip','city','state','updated')
                .write
                .mode("append")
                .format("json")
                .save(self.userdir))
            self.batch += 1
            
        print(f"({int(time.time())-start} seconds)")

DA.data_factory = CdcDataFactory()

# COMMAND ----------

DA.cleanup()
DA.init()

# COMMAND ----------

# Create the source datasets
create_source_database()          # Create the source database
create_producer_table_source()    # Clone the producer table
create_date_lookup_source()
create_user_lookup_source()
create_cdc_raw_source()
print()

# Create the user datasets
# create_date_lookup()              # Create static copy of date_lookup
# create_user_lookup()              # Create the user-lookup table
# print()

# init_source_daily()               # Create the data factory
# DA.data_factory.load()            # Load one new day for DA.paths.source_daily

# DA.process_bronze()               # Process through the bronze table
# DA.process_heart_rate_silver()    # Process the heart_rate_silver table
# DA.process_workouts_silver()      # Process the workouts_silver table
# DA.process_completed_workouts()   # Process the completed_workouts table
# DA.process_workout_bpm()

# DA.process_users()
# DA.process_user_bins()

# COMMAND ----------

DA.conclude_setup()

