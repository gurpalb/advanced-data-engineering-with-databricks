# Databricks notebook source
def create_source_database():
    DA.source_db_name = f"{DA.db_name_prefix}_source"
    DA.hidden.source_db = f"{DA.working_dir_prefix}/source/source.db"

    ################################
    # Creating the source database
    print(f"Creating the database \"{DA.source_db_name}\"")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DA.source_db_name} LOCATION '{DA.hidden.source_db}'")
    
None # Suppressing Output

# COMMAND ----------

def create_producer_table_source():
    import time
    
    ################################
    # Cloning producer table
    start = int(time.time())
    print(f"Cloning to source database producer", end="...")
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {DA.source_db_name}.producer
      DEEP CLONE delta.`{DA.data_source_uri}/bronze`
    """) # No location for source db
    print(f"({int(time.time())-start} seconds)")

None # Suppressing Output

# COMMAND ----------

def create_date_lookup_source():
    import time
    
    # The first clone brings it from Azure into the workspace
    start = int(time.time())
    print(f"Cloning to source database date_lookup", end="...")
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {DA.source_db_name}.date_lookup
      DEEP CLONE delta.`{DA.data_source_uri}/date-lookup`
    """) # No location for source db
    print(f"({int(time.time())-start} seconds)")

    
def create_date_lookup():
    import time
    # The second clone moves it from the source DB to the user DB
    start = int(time.time())
    print(f"Creating date_lookup", end="...")
    
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS date_lookup
      DEEP CLONE {DA.source_db_name}.date_lookup
      LOCATION '{DA.paths.user_db}/date_lookup'
    """)
    
    total = spark.read.table("date_lookup").count()
    print(f"({int(time.time())-start} seconds / {total:,} records)")
    
None # Suppressing Output

# COMMAND ----------

class DailyDataFactory:
    def __init__(self, target_dir, reset=True, starting_batch=1, max_batch=16):
        from pyspark.sql import functions as F
        
        self.rawDF = spark.table(f"{DA.source_db_name}.producer").withColumn("day", F.when(F.col("date") <= '2019-12-01', 1).otherwise(F.dayofmonth("date")))
        self.target_dir = target_dir
        self.max_batch = max_batch
        
        if reset == True:
            self.batch = starting_batch
            dbutils.fs.rm(self.target_dir, True)
        else:
            self.batch = spark.read.json(DA.paths.source_daily).select(F.max(F.when((F.col("timestamp")/1000).cast("timestamp").cast("date") <= '2019-12-01', 1).otherwise(F.dayofmonth((F.col("timestamp")/1000).cast("timestamp").cast("date"))))).collect()[0][0]
            
    def load(self, continuous=False, silent=False):
        import time
        start = int(time.time())
        
        if self.batch > self.max_batch:
            print("Data source exhausted\n")
            
        elif continuous == True:
            print("Loading all streaming data", end="...")
            self.load_batch(self.batch, self.max_batch)
            self.batch = self.max_batch+1
            
        else:
            if not silent: print(f"Loading batch #{self.batch} to daily stream", end="...")
            self.load_batch(self.batch, self.batch+1)
            self.batch += 1
            
        print(f"({int(time.time())-start} seconds)")
    
    def load_batch(self, min_batch, max_batch):
        from pyspark.sql import functions as F
        (self.rawDF
             .filter(F.col("day") >= min_batch)
             .filter(F.col("day") <= max_batch)
             .drop("date", "week_part", "day")
             .write.mode("append").format("json").save(self.target_dir))

None # Suppressing Output

# COMMAND ----------

def init_source_daily():
    DA.paths.source_daily = f"{DA.paths.working_dir}/streams/daily.json"
    DA.data_factory = DailyDataFactory(DA.paths.source_daily, reset=True)
    
None # Suppressing Output

# COMMAND ----------

# This is the solution from lesson 2.03 and is included
# here to fast-forward the student to this stage
def _process_bronze():
    import time
    from pyspark.sql import functions as F
    from pyspark.sql.utils import AnalysisException

    start = int(time.time())
    print(f"Processing the bronze table from the daily stream", end="...")
        
    schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"
    date_lookup_df = spark.table("date_lookup").select("date", "week_part")

    def execute_stream():
        (spark.readStream
              .format("cloudFiles")
              .schema(schema)
              .option("cloudFiles.format", "json")
              .load(DA.paths.source_daily)
              .join(F.broadcast(date_lookup_df), F.to_date((F.col("timestamp")/1000).cast("timestamp")) == F.col("date"), "left")
              .writeStream
              .option("checkpointLocation", f"{DA.paths.checkpoints}/bronze.chk")
              .partitionBy("topic", "week_part")
              .option("path", f"{DA.paths.user_db}/bronze")
              .trigger(once=True)
              .table("bronze")
              .awaitTermination())
    
    # The cluster is going to cache the state of the stream and it will be wrong.
    # But it will also invalidate that cache allowing us to try again.
    try: execute_stream()
    except AnalysisException: execute_stream()
    
    total = spark.read.table("bronze").count()
    print(f"({int(time.time())-start} seconds / {total:,} records)")
    
DA.process_bronze = _process_bronze

None # Suppressing Output

# COMMAND ----------

def _process_heart_rate_silver_v0():
    import time
    from pyspark.sql import functions as F
    from pyspark.sql.utils import AnalysisException

    start = int(time.time())
    print("Processing the heart_rate_silver table", end="...")

    class Upsert:
        def __init__(self, sql_query, update_temp="stream_updates"):
            self.sql_query = sql_query
            self.update_temp = update_temp 

        def upsertToDelta(self, microBatchDF, batch):
            microBatchDF.createOrReplaceTempView(self.update_temp)
            microBatchDF._jdf.sparkSession().sql(self.sql_query)
    
    spark.sql("CREATE TABLE IF NOT EXISTS heart_rate_silver (device_id LONG, time TIMESTAMP, heartrate DOUBLE) USING DELTA")
    
    streamingMerge=Upsert("""
      MERGE INTO heart_rate_silver a
      USING stream_updates b
      ON a.device_id=b.device_id AND a.time=b.time
      WHEN NOT MATCHED THEN INSERT *
    """)

    def execute_stream():
        (spark.readStream
              .table("bronze")
              .filter("topic = 'bpm'")
              .select(F.from_json(F.col("value").cast("string"), "device_id LONG, time TIMESTAMP, heartrate DOUBLE").alias("v"))
              .select("v.*")
              .withWatermark("time", "30 seconds")
              .dropDuplicates(["device_id", "time"])
              .writeStream
              .foreachBatch(streamingMerge.upsertToDelta)
              .outputMode("update")
              .option("checkpointLocation", f"{DA.paths.checkpoints}/heart_rate.chk")
              .trigger(once=True)
              .start()
              .awaitTermination())
    
    # The cluster is going to cache the state of the stream and it will be wrong.
    # But it will also invalidate that cache allowing us to try again.
    try: execute_stream()
    except AnalysisException: execute_stream()
    
    total = spark.read.table("heart_rate_silver").count() 
    print(f"({int(time.time())-start} seconds / {total:,} records)")

DA.process_heart_rate_silver_v0 = _process_heart_rate_silver_v0

None # Suppressing Output

# COMMAND ----------

def _process_heart_rate_silver():
    import time
    from pyspark.sql import functions as F
    from pyspark.sql.utils import AnalysisException

    start = int(time.time())
    print("Processing the heart_rate_silver table", end="...")

    class Upsert:
        def __init__(self, sql_query, update_temp="stream_updates"):
            self.sql_query = sql_query
            self.update_temp = update_temp 

        def upsertToDelta(self, microBatchDF, batch):
            microBatchDF.createOrReplaceTempView(self.update_temp)
            microBatchDF._jdf.sparkSession().sql(self.sql_query)
    
    spark.sql("CREATE TABLE IF NOT EXISTS heart_rate_silver (device_id LONG, time TIMESTAMP, heartrate DOUBLE, bpm_check STRING) USING DELTA")
    
    streamingMerge=Upsert("""
      MERGE INTO heart_rate_silver a
      USING stream_updates b
      ON a.device_id=b.device_id AND a.time=b.time
      WHEN NOT MATCHED THEN INSERT *
    """)

    def execute_stream():
        (spark.readStream
              .table("bronze")
              .filter("topic = 'bpm'")
              .select(F.from_json(F.col("value").cast("string"), "device_id LONG, time TIMESTAMP, heartrate DOUBLE").alias("v"))
              .select("v.*", F.when(F.col("v.heartrate") <= 0, "Negative BPM").otherwise("OK").alias("bpm_check"))
              .withWatermark("time", "30 seconds")
              .dropDuplicates(["device_id", "time"])
              .writeStream
              .foreachBatch(streamingMerge.upsertToDelta)
              .outputMode("update")
              .option("checkpointLocation", f"{DA.paths.checkpoints}/heart_rate.chk")
              .trigger(once=True)
              .start()
              .awaitTermination())    
    
    # The cluster is going to cache the state of the stream and it will be wrong.
    # But it will also invalidate that cache allowing us to try again.
    try: execute_stream()
    except AnalysisException: execute_stream()
        
    total = spark.read.table("heart_rate_silver").count() 
    print(f"({int(time.time())-start} seconds / {total:,} records)")

DA.process_heart_rate_silver = _process_heart_rate_silver

None # Suppressing Output

# COMMAND ----------

def _process_workouts_silver(once=False, processing_time="15 seconds"):
    import time
    from pyspark.sql import functions as F
    from pyspark.sql.utils import AnalysisException

    start = int(time.time())
    print("Processing the workouts_silver table", end="...")
    
    spark.sql(f"CREATE TABLE IF NOT EXISTS workouts_silver (user_id INT, workout_id INT, time TIMESTAMP, action STRING, session_id INT) USING DELTA")
    
    class Upsert:
        def __init__(self, query, update_temp="stream_updates"):
            self.query = query
            self.update_temp = update_temp 

        def upsertToDelta(self, microBatchDF, batch):
            microBatchDF.createOrReplaceTempView(self.update_temp)
            microBatchDF._jdf.sparkSession().sql(self.query)
    
    sql_query = """
        MERGE INTO workouts_silver a
        USING workout_updates b
        ON a.user_id=b.user_id AND a.time=b.time
        WHEN NOT MATCHED THEN INSERT *
        """

    streamingMerge=Upsert(sql_query, "workout_updates")
    
    def execute_stream():
        (spark.readStream
              .option("ignoreDeletes", True)
              .table("bronze")
              .filter("topic = 'workout'")
              .select(F.from_json(F.col("value").cast("string"), "user_id INT, workout_id INT, timestamp FLOAT, action STRING, session_id INT").alias("v"))
              .select("v.*")
              .select("user_id", "workout_id", F.col("timestamp").cast("timestamp").alias("time"), "action", "session_id")
              .withWatermark("time", "30 seconds")
              .dropDuplicates(["user_id", "time"])
              .writeStream
              .foreachBatch(streamingMerge.upsertToDelta)
              .outputMode("update")
              .option("checkpointLocation", f"{DA.paths.checkpoints}/workouts.chk")
              .queryName("workouts_silver")
              .trigger(once=True)
              .start()
              .awaitTermination())
        
    # The cluster is going to cache the state of the stream and it will be wrong.
    # But it will also invalidate that cache allowing us to try again.
    try: execute_stream()
    except AnalysisException: execute_stream()
    
    total = spark.read.table("workouts_silver").count() 
    print(f"({int(time.time())-start} seconds / {total:,} records)")
    
DA.process_workouts_silver = _process_workouts_silver

None # Suppressing Output

# COMMAND ----------

def _process_completed_workouts():
    import time
    from pyspark.sql import functions as F

    start = int(time.time())
    print("Processing the completed_workouts table", end="...")

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW TEMP_completed_workouts AS (
          SELECT a.user_id, a.workout_id, a.session_id, a.start_time start_time, b.end_time end_time, a.in_progress AND (b.in_progress IS NULL) in_progress
          FROM (
            SELECT user_id, workout_id, session_id, time start_time, null end_time, true in_progress
            FROM workouts_silver
            WHERE action = "start") a
          LEFT JOIN (
            SELECT user_id, workout_id, session_id, null start_time, time end_time, false in_progress
            FROM workouts_silver
            WHERE action = "stop") b
          ON a.user_id = b.user_id AND a.session_id = b.session_id
        )
    """)
    
    (spark.table("TEMP_completed_workouts").write.mode("overwrite").saveAsTable("completed_workouts"))
    
    total = spark.read.table("completed_workouts").count() 
    print(f"({int(time.time())-start} seconds / {total:,} records)")
    
DA.process_completed_workouts = _process_completed_workouts

None # Suppressing Output

# COMMAND ----------

def _process_workout_bpm():
    import time
    from pyspark.sql.utils import AnalysisException
    
    start = int(time.time())
    print("Processing the workout_bpm table", end="...")

    spark.readStream.table("heart_rate_silver").createOrReplaceTempView("TEMP_heart_rate_silver")
    
    spark.sql("""
        SELECT d.user_id, d.workout_id, d.session_id, time, heartrate
        FROM TEMP_heart_rate_silver c
        INNER JOIN (
          SELECT a.user_id, b.device_id, workout_id, session_id, start_time, end_time
          FROM completed_workouts a
          INNER JOIN user_lookup b
          ON a.user_id = b.user_id) d
        ON c.device_id = d.device_id AND time BETWEEN start_time AND end_time
        WHERE c.bpm_check = 'OK'""").createOrReplaceTempView("TEMP_workout_bpm")
    
    def execute_stream():
        (spark.table("TEMP_workout_bpm")
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{DA.paths.checkpoints}/workout_bpm.chk")
            .option("path", f"{DA.paths.user_db}/workout_bpm")
            .trigger(once=True)
            .table("workout_bpm")
            .awaitTermination())
    
    # The cluster is going to cache the state of the stream and it will be wrong.
    # But it will also invalidate that cache allowing us to try again.
    try: execute_stream()
    except AnalysisException: execute_stream()

    total = spark.read.table("workout_bpm").count() 
    print(f"({int(time.time())-start} seconds / {total:,} records)")
    
DA.process_workout_bpm = _process_workout_bpm

None # Suppressing Output    

# COMMAND ----------

def create_user_lookup_source():
    import time
    
    # The first clone brings it from Azure into the workspace
    start = int(time.time())
    print(f"Cloning to source database user_lookup", end="...")
    
    if spark.sql(f"SHOW TABLES IN {DA.source_db_name}").filter(f"tableName = 'user_lookup'").count() == 0:
        (spark.read
              .format("json")
              .schema("device_id long, mac_address string, registration_timestamp double, user_id long")
              .load(f"{DA.data_source_uri}/user-reg")
              .selectExpr(f"sha2(concat(user_id,'BEANS'), 256) AS alt_id", "device_id", "mac_address", "user_id")
              .write
              # .option("path", xxx) # No location for source tables
              .mode("overwrite")
              .saveAsTable(f"{DA.source_db_name}.user_lookup"))

    print(f"({int(time.time())-start} seconds)")
    
    
def create_user_lookup():
    import time
    
    # The first clone brings it from Azure into the workspace
    start = int(time.time())
    print(f"Creating user_lookup", end="...")
    
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS user_lookup
      DEEP CLONE {DA.source_db_name}.user_lookup
      LOCATION '{DA.paths.user_db}/user_lookup'
    """)

    total = spark.sql(f"SELECT * FROM user_lookup").count()
    print(f"({int(time.time())-start} seconds / {total:,} records)")    
    
None # Suppressing Output

# COMMAND ----------

def batch_rank_upsert(microBatchDF, batchId):
    from pyspark.sql.window import Window
    from pyspark.sql import functions as F

    window = Window.partitionBy("alt_id").orderBy(F.col("updated").desc())
    
    (microBatchDF
        .filter(F.col("update_type").isin(["new", "update"]))
        .withColumn("rank", F.rank().over(window))
        .filter("rank == 1")
        .drop("rank")
        .createOrReplaceTempView("ranked_updates"))
    
    microBatchDF._jdf.sparkSession().sql("""
        MERGE INTO users u
        USING ranked_updates r
        ON u.alt_id=r.alt_id
        WHEN MATCHED AND u.updated < r.updated
          THEN UPDATE SET *
        WHEN NOT MATCHED
          THEN INSERT *
    """)

    (microBatchDF
        .filter("update_type = 'delete'")
        .select("alt_id", 
                F.col("updated").alias("requested"), 
                F.date_add("updated", 30).alias("deadline"), 
                F.lit("requested").alias("status"))
        .write
        .format("delta")
        .mode("append")
        .option("txnVersion", batchId)
        .option("txnAppId", "batch_rank_upsert")
        .option("path", f"{DA.paths.user_db}/delete_requests")
        .saveAsTable("delete_requests"))
    
def _process_users():
    import time
    from pyspark.sql import functions as F
    from pyspark.sql.utils import AnalysisException
    
    start = int(time.time())
    print(f"Processing the users table", end="...")

    spark.sql(f"CREATE TABLE IF NOT EXISTS users (alt_id STRING, dob DATE, sex STRING, gender STRING, first_name STRING, last_name STRING, street_address STRING, city STRING, state STRING, zip INT, updated TIMESTAMP) USING DELTA")
    
    schema = """
        user_id LONG, 
        update_type STRING, 
        timestamp FLOAT, 
        dob STRING, 
        sex STRING, 
        gender STRING, 
        first_name STRING, 
        last_name STRING, 
        address STRUCT<
            street_address: STRING, 
            city: STRING, 
            state: STRING, 
            zip: INT
    >"""
    
    def execute_stream():
        (spark.readStream
            .table("bronze")
            .filter("topic = 'user_info'")
            .dropDuplicates()
            .select(F.from_json(F.col("value").cast("string"), schema).alias("v")).select("v.*")
            .select(F.sha2(F.concat(F.col("user_id"), F.lit("BEANS")), 256).alias("alt_id"),
                F.col('timestamp').cast("timestamp").alias("updated"),
                F.to_date('dob','MM/dd/yyyy').alias('dob'),
                'sex', 'gender','first_name','last_name',
                'address.*', "update_type")
            .writeStream
            .foreachBatch(batch_rank_upsert)
            .outputMode("update")
            .option("checkpointLocation", f"{DA.paths.checkpoints}/users.chk")
            .trigger(once=True)
            .start()
            .awaitTermination())    

    # The cluster is going to cache the state of the stream and it will be wrong.
    # But it will also invalidate that cache allowing us to try again.
    try: execute_stream()
    except AnalysisException: execute_stream()
        
    print(f"({int(time.time())-start} seconds)")

#     total = spark.read.table("ranked_updates").count()
#     print(f"...ranked_updates: {total} records)")

    total = spark.read.table("delete_requests").count()
    print(f"...delete_requests: {total} records)")

    total = spark.read.table("users").count()
    print(f"...users: {total} records)")
    
DA.process_users = _process_users
    
None # Suppressing Output

# COMMAND ----------

def age_bins(dob_col):
    from pyspark.sql import functions as F

    age_col = F.floor(F.months_between(F.current_date(), dob_col)/12).alias("age")
    return (F.when((age_col < 18), "under 18")
            .when((age_col >= 18) & (age_col < 25), "18-25")
            .when((age_col >= 25) & (age_col < 35), "25-35")
            .when((age_col >= 35) & (age_col < 45), "35-45")
            .when((age_col >= 45) & (age_col < 55), "45-55")
            .when((age_col >= 55) & (age_col < 65), "55-65")
            .when((age_col >= 65) & (age_col < 75), "65-75")
            .when((age_col >= 75) & (age_col < 85), "75-85")
            .when((age_col >= 85) & (age_col < 95), "85-95")
            .when((age_col >= 95), "95+")
            .otherwise("invalid age").alias("age"))

def _process_user_bins():
    import time
    from pyspark.sql import functions as F

    start = int(time.time())
    print(f"Processing user_bins table", end="...")
    
    (spark.table("users")
         .join(
            spark.table("user_lookup")
                .select("alt_id", "user_id"), 
            ["alt_id"], 
            "left")
        .select("user_id", 
                age_bins(F.col("dob")),
                "gender", 
                "city", 
                "state")
        .write
        .format("delta")
        .option("path", f"{DA.paths.user_db}/user_bins")
        .mode("overwrite")
        .saveAsTable("user_bins"))
    
    total = spark.read.table("user_bins").count()
    print(f"({int(time.time())-start} seconds / {total:,} records)")
    

DA.process_user_bins = _process_user_bins    

None # Suppressing Output

# COMMAND ----------

def create_gym_mac_logs():
    import time
    start = int(time.time())
    print(f"Creating gym_mac_logs", end="...")

    (spark.read
          .json(f"{DA.data_source_uri}/gym-logs")
          .write
          .option("path", f"{DA.paths.user_db}/gym-logs")
          .saveAsTable("gym_mac_logs"))
     
    total = spark.read.table("gym_mac_logs").count()
    print(f"({int(time.time())-start} seconds / {total:,} records)")
     

