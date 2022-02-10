# Databricks notebook source
# MAGIC %run ../_databricks-academy-helper $lesson="1.02"

# COMMAND ----------

def create_raw_data():
    from pyspark.sql import functions as F
    (spark.read
          .load(f"{DA.data_source_uri}/bronze")
          .select(F.col("key").cast("string").alias("key"), "value", "topic", "partition", "offset", "timestamp")
          .filter("week_part > '2019-49'")
          .write
          .format("parquet")
          .save(f"{DA.paths.working_dir}/raw_parquet"))

    DA.raw_data_tbl = "raw_data" 
    spark.conf.set("da.raw_data_tbl", DA.raw_data_tbl)
    spark.read.format("parquet").load(f"{DA.paths.working_dir}/raw_parquet").createOrReplaceTempView(DA.raw_data_tbl)


# COMMAND ----------

DA.cleanup()
DA.init()

create_raw_data()

DA.conclude_setup()

