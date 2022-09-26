-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS LAB;
USE DATABASE LAB;

-- COMMAND ----------

CREATE OR REPLACE TABLE config_pdb_actualizer
(
  experiment_id STRING,
  is_forced BOOLEAN
)
;

INSERT INTO config_pdb_actualizer VALUES
    ("100d", FALSE),
    ("402d", TRUE)
;

-- COMMAND ----------

CREATE OR REPLACE TABLE register_pdb_actualizer
(
  experiment_id STRING NOT NULL,
  run_date TIMESTAMP NOT NULL,
  hash_sum STRING NOT NULL  
)

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze_entity
(
  seq_id LONG GENERATED ALWAYS AS IDENTITY,
  experiment_id STRING
);

CREATE OR REPLACE TABLE bronze_chemp_comp
(
  seq_id LONG GENERATED ALWAYS AS IDENTITY,
  experiment_id STRING
);

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SHOW DATABASES
