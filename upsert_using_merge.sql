-- Databricks notebook source
CREATE TABLE Source_Table
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

INSERT INTO Source_Table
VALUES
  ('Bachelor', 100, 4000, 500, 'Networking', 'Male', '2023-07-12', 1),
  ('Bachelor', 101, 4000, 500, 'Networking', 'Male', '2023-07-12', 1),
  ('Bachelor', 102, 4000, 500, 'Networking', 'Male', '2023-07-12', 1);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###creating a destination_table

-- COMMAND ----------


CREATE TABLE delta.Dest_Table
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

INSERT INTO delta.Dest_Table
VALUES
  ('Bachelor', 100, 4500, 500, 'Networking', 'Male', '2023-07-12', 1),
  ('Masters', 101, 5000, 500, 'Networking', 'Male', '2023-07-12', 1);

-- COMMAND ----------

SELECT * FROM Source_Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##upsert using merge
-- MAGIC •**Updating** and **Merging** the records from score_table to dest_table at the same time 

-- COMMAND ----------

MERGE INTO `delta`.Dest_Table AS Dest
USING Source_Table as Source
    on Dest.Line_Number = Source.Line_Number
  WHEN MATCHED
    THEN UPDATE SET
  Dest.Education_Level = Source.Education_Level,
  Dest.Line_Number = Source.Line_Number,
  Dest.Employed = Source.Employed,
  Dest.Unemployed = Source.Unemployed,
  Dest.Industry = Source.Industry,
  Dest.Gender = Source.Gender,
  Dest.Date_Inserted = Source.Date_Inserted,
  Dest.dense_rank = Source.dense_rank

  WHEN NOT MATCHED
  THEN INSERT
    (Education_Level, Line_Number, Employed, Unemployed, Industry, Gender, Date_Inserted, dense_rank)
    VALUES(Source.Education_Level, Source.Line_Number, Source.Employed, Source.Unemployed, Source.Industry, Source.Gender, Source.Date_Inserted, Source.dense_rank)

-- COMMAND ----------

SELECT * FROM delta.Dest_Table

-- COMMAND ----------


