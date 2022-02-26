-- Databricks notebook source
-- MAGIC %python
-- MAGIC import re
-- MAGIC username = spark.sql("SELECT current_user()").first()[0]
-- MAGIC clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
-- MAGIC spark.conf.set("da.database", f"dbacademy_{clean_username}_adewd_jobs")

-- COMMAND ----------

DROP DATABASE IF EXISTS ${da.database} CASCADE;

CREATE DATABASE IF NOT EXISTS ${da.database};
USE ${da.database}

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS task_1
(run_time TIMESTAMP);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS task_2
(task STRING, run_time TIMESTAMP);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS task_3
(task STRING, run_time TIMESTAMP);

-- COMMAND ----------

INSERT INTO task_1 VALUES (current_timestamp());

-- COMMAND ----------

SELECT COUNT(*) FROM task_1

-- COMMAND ----------

SELECT * FROM task_1

