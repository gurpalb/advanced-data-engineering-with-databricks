# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Factory
# MAGIC 
# MAGIC This notebook is used to load data to the source directory that feeds our bronze table from [2 - Schedule Streaming Jobs]($./2 - Schedule Streaming Jobs)
# MAGIC 
# MAGIC Simply hit **Run All** above and then go back and review the output of the job.

# COMMAND ----------

# MAGIC %run ../../Includes/module-4/setup-lesson-4.03.99-data-factory

# COMMAND ----------

from pyspark.sql import functions as F

batch = 0
producer_df = spark.read.table(f"{DA.source_db_name}.producer_30m")
arrival_max, arrival_min = producer_df.select(F.max("arrival"), F.min("arrival")).collect()[0]

print(f"arrival_min:  {arrival_min:,}")
print(f"arrival_max:  {arrival_max:,}")
print(f"Total Batchs: {arrival_max-arrival_min:,}")

# COMMAND ----------

import time

while arrival_min+batch < arrival_max+1:
    start = time.time()*1000
    
    (producer_df.filter(F.col("arrival") == arrival_min+batch).drop("arrival")
                .write
                .mode("append")
                .format("json")
                .save(DA.paths.producer_30m))
    
    print(f"Batch #{batch+1} duration: {int(time.time()*1000-start):,} ms")
    batch += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wraping Up
# MAGIC 
# MAGIC * Stop/Pause the jobs
# MAGIC * Run the following cell to delete the tables and files associated with this lesson.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
