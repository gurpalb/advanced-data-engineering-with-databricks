# Databricks notebook source
# MAGIC %run ../_databricks-academy-helper $lesson="3.03"

# COMMAND ----------

# MAGIC %run ../_utility-functions

# COMMAND ----------

DA.cleanup()
DA.init()

# COMMAND ----------

def create_bronze_table():
    import time
    
    # This is the exact same datasource used by create_producer_table_source()
    # The difference is that one is in the source database and this one will in the user database
    
    start = int(time.time())
    print(f"Creating bronze", end="...")
    
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS bronze
      DEEP CLONE delta.`{DA.data_source_uri}/bronze`
      LOCATION '{DA.paths.user_db}/bronze'
    """) 
    
    print(f"({int(time.time())-start} seconds)")

None # Suppressing Output

# COMMAND ----------

def create_user_lookup_source_v2_source():
    import time
    from pyspark.sql import functions as F
    
    DA.hidden.users_producer = f"{DA.hidden.source_db}/users_producer"

    if not DA.paths.exists(DA.hidden.users_producer):
        (spark.read
              .format("json")
              .schema("device_id long, mac_address string, registration_timestamp double, user_id long")
              .load(f"{DA.data_source_uri}/user-reg")
              .select("*", F.col("registration_timestamp").cast("timestamp").cast("date").alias("date"))
              .createOrReplaceTempView("temp_table"))

        spark.sql("""
            CREATE OR REPLACE TEMP VIEW final_df AS
            SELECT device_id, mac_address, registration_timestamp, user_id, CASE
              WHEN date < "2019-12-01"
                THEN 0
              ELSE dayofmonth(date)
              END AS batch
            FROM temp_table
        """)

        spark.table("final_df").write.save(DA.hidden.users_producer)
        spark.sql("DROP TABLE final_df")

# COMMAND ----------

class DataFactory:
    def __init__(self, demohome, reset=True, starting_batch=0, max_batch=15):
        self.userdir = demohome
        self.batch = starting_batch
        self.max_batch = max_batch
        if reset == True:
            dbutils.fs.rm(self.userdir, True)
            
    def load(self, continuous=False):
        import time
        from pyspark.sql import functions as F

        start = int(time.time())
        rawDF = spark.read.load(DA.hidden.users_producer)
        
        if self.batch > self.max_batch:
            print("Data source exhausted\n")
            
        elif continuous == True:
            print("Loading all batches to raw_user_reg", end="...")
            while self.batch <= self.max_batch:
                print(f"Loading batch #{self.batch} to raw_user_reg", end="...")
                (rawDF.filter(F.col("batch") == self.batch).drop("batch")
                    .write
                    .mode("append")
                    .format("json")
                    .save(self.userdir))
                self.batch += 1
                print(f"({int(time.time())-start} seconds)")
        else:
            print(f"Loading batch #{self.batch} to raw_user_reg", end="...")
            (rawDF.filter(F.col("batch") == self.batch).drop("batch")
                .write
                .mode("append")
                .format("json")
                .save(self.userdir))
            self.batch += 1
            print(f"({int(time.time())-start} seconds)")
            
DA.paths.raw_user_reg = f"{DA.paths.user_db}/pii/raw_user_reg"
DA.data_factory = DataFactory(DA.paths.raw_user_reg)

# COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)

# # Create the source datasets
create_source_database()          # Create the source database
#create_producer_table_source()    # Clone the producer table
create_date_lookup_source()
# create_user_lookup_source()
print()

# # Create the user datasets
create_date_lookup()              # Create static copy of date_lookup
# create_user_lookup()              # Create the user-lookup table
create_bronze_table()
create_user_lookup_source_v2_source()
print()

# init_source_daily()               # Create the data factory
DA.data_factory.load()               # Load one new day for DA.paths.source_daily

# DA.process_bronze()               # Process through the bronze table
# DA.process_heart_rate_silver()    # Process the heart_rate_silver table
# DA.process_workouts_silver()      # Process the workouts_silver table
# DA.process_completed_workouts()   # Process the completed_workouts table

# DA.process_users()
# DA.process_user_bins()

# COMMAND ----------

DA.conclude_setup()

# COMMAND ----------

# %sql drop database dbacademy_jacob_parr_databricks_com_adewd_source cascade

