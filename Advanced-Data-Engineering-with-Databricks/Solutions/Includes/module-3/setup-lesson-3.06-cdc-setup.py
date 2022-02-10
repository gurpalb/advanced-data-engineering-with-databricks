# Databricks notebook source
# MAGIC %run ../_databricks-academy-helper $lesson="3.06"

# COMMAND ----------

# MAGIC %run ../_utility-functions

# COMMAND ----------

def create_cdc_raw():
    import time
    
    start = int(time.time())
    print(f"Creating cdc_raw", end="...")
    
    spark.sql(f"""CREATE TABLE cdc_raw
                  DEEP CLONE delta.`{data_source_uri}/pii/raw`
                  LOCATION '{DA.paths.user_db}/cdc_raw.delta'
                  
    total = spark.read.table("cdc_raw").count()
    print(f"({int(time.time())-start} seconds / {total:,} records)")
""")

# COMMAND ----------

class CdcDataFactory:
    def __init__(self, demohome, reset=True, max_batch=3):
        self.rawDF = spark.read.format("delta").load(filepath)
        self.userdir = demohome
        self.batch = 1
        self.max_batch = max_batch
        if reset == True:
            dbutils.fs.rm(self.userdir, True)
            
    def load(self, continuous=False):
        if self.batch > self.max_batch:
            print("Data source exhausted\n")
        elif continuous == True:
            while self.batch <= max_batch:
                (self.rawDF.filter(F.col("batch") == self.batch)
                    .select('mrn','dob','sex','gender','first_name','last_name','street_address','zip','city','state','updated')
                    .write
                    .mode("append")
                    .format("json")
                    .save(self.userdir))
                self.batch += 1
        else:
            (self.rawDF.filter(F.col("batch") == self.batch)
                .select('mrn','dob','sex','gender','first_name','last_name','street_address','zip','city','state','updated')
                .write
                .mode("append")
                .format("json")
                .save(self.userdir))
            self.batch += 1


# COMMAND ----------

def init_cdc_data_factory():
    DA.paths.source_cdc = f"{DA.paths.working_dir}/streams/cdc.json"
    DA.data_factory = CdcDataFactory(DA.paths.source_cdc)
    
None # Suppressing Output

# COMMAND ----------

DA.cleanup()
DA.init()

# COMMAND ----------

# Create the source datasets
create_source_database()          # Create the source database
create_producer_table_source()    # Clone the producer table
create_date_lookup_source()
create_user_lookup_source()
create_cdc_raw()
print()

# Create the user datasets
create_date_lookup()              # Create static copy of date_lookup
create_user_lookup()              # Create the user-lookup table
print()

init_source_daily()               # Create the data factory
DA.data_factory.load()            # Load one new day for DA.paths.source_daily

DA.process_bronze()               # Process through the bronze table
# DA.process_heart_rate_silver()    # Process the heart_rate_silver table
# DA.process_workouts_silver()      # Process the workouts_silver table
# DA.process_completed_workouts()   # Process the completed_workouts table
# DA.process_workout_bpm()

# DA.process_users()
# build_user_bins()

# COMMAND ----------

DA.conclude_setup()

