-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## How to use the optimize command

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Creating a delta table

-- COMMAND ----------

CREATE TABLE delta.OptimizeTable
(
  Education_level varchar(50),
  Line_Number INT,
  EMPLOYED INT,
  UNEMPLOYED INT,
  INDUSTRY VARCHAR(50),
  GENDER VARCHAR(10),
  DATE_INSERTED DATE,
  dense_rank INT
)USING DELTA;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log/')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.format('csv').load('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log/00000000000000000000.json'))

-- COMMAND ----------

DESCRIBE EXTENDED  delta.OptimizeTable

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Inserting fist record into table

-- COMMAND ----------

INSERT INTO delta.OptimizeTable
VALUES
  ('Bachelor', 100, 4000, 500, 'Netwoorking', 'Male', '2023-07-12', 1)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log/')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.format('text').load('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log/00000000000000000001.json'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##inserting second record

-- COMMAND ----------

INSERT INTO delta.OptimizeTable
VALUES
  ('Bachelor', 102, 4000, 500, 'Netwoorking', 'Male', '2023-07-12', 1)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log/')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.format('text').load('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log/00000000000000000002.json'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##inserting thrid record

-- COMMAND ----------

INSERT INTO delta.OptimizeTable
VALUES
  ('Bachelor', 103, 4000, 500, 'Netwoorking', 'Male', '2023-07-12', 1)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.format('text').load('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log/00000000000000000003.json'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##listing all the parquet files

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/delta.db/optimizetable/

-- COMMAND ----------

-- deleting a record
DELETE FROM delta.OptimizeTable
WHERE Line_Number = 101

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##listing the log files

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log/')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.format('csv').load('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log/00000000000000000004.json'))

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/delta.db/optimizetable/

-- COMMAND ----------

-- updating a record
UPDATE delta.OptimizeTable
SET Line_Number = 99
WHERE Line_Number = 102

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.format('text').load('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log/00000000000000000005.json'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####After optimizing instead of reading through the multiple parquet files, records can be fetched from a single file. A single parquet file will be created from by removing all active parquet files. A max file size of 1GB can be generated. In this way, to fetch query results, opening and closing multiple files will be avoided.

-- COMMAND ----------

OPTIMIZE delta.OptimizeTable

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.format('text').load('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log/00000000000000000006.json'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC •Eventhough active files are merged into a single file, user can still levarage the **Time Travel** feature.

-- COMMAND ----------

DESCRIBE HISTORY `delta`.OptimizeTable

-- COMMAND ----------

SELECT * FROM `delta`.OptimizeTable VERSION AS OF 4

-- COMMAND ----------

SELECT * FROM `delta`.OptimizeTable

-- COMMAND ----------


