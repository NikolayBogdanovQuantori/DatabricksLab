-- Databricks notebook source
DROP DATABASE IF EXISTS LAB CASCADE;

CREATE DATABASE LAB;

-- COMMAND ----------

USE DATABASE LAB;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS config_pdb_actualizer
(
  experiment_id STRING,
  is_forced BOOLEAN
)
;

-- COMMAND ----------

CREATE OR REPLACE TABLE register_pdb_actualizer
(
  experiment_id STRING NOT NULL,
  updated_ts TIMESTAMP NOT NULL,
  hash_sum STRING NOT NULL  
)

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze_entity
(
  seq_id LONG GENERATED ALWAYS AS IDENTITY,
  updated_ts TIMESTAMP NOT NULL,
  experiment_id STRING
)
PARTITIONED BY(experiment_id)
;


-- COMMAND ----------

CREATE OR REPLACE TABLE bronze_pdbx_database_PDB_obs_spr
(
  seq_id LONG GENERATED ALWAYS AS IDENTITY,
  updated_ts TIMESTAMP NOT NULL,
  experiment_id STRING
)
PARTITIONED BY(experiment_id)
;

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze_entity_poly_seq
(
  seq_id LONG GENERATED ALWAYS AS IDENTITY,
  updated_ts TIMESTAMP NOT NULL,
  experiment_id STRING
)
PARTITIONED BY(experiment_id)
;

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze_chem_comp
(
  seq_id LONG GENERATED ALWAYS AS IDENTITY,
  updated_ts TIMESTAMP NOT NULL,
  experiment_id STRING
)
PARTITIONED BY(experiment_id)
;

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze_exptl
(
  seq_id LONG GENERATED ALWAYS AS IDENTITY,
  updated_ts TIMESTAMP NOT NULL,
  experiment_id STRING
)
PARTITIONED BY(experiment_id)
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC SILVER PART:

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_entity
(
  seq_id LONG,
  updated_ts TIMESTAMP NOT NULL,
  experiment_id STRING
)
PARTITIONED BY(experiment_id)
;

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_pdbx_database_PDB_obs_spr
(
  seq_id LONG,
  updated_ts TIMESTAMP NOT NULL,
  experiment_id STRING
)
PARTITIONED BY(experiment_id)
;

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_entity_poly_seq
(
  seq_id LONG,
  updated_ts TIMESTAMP NOT NULL,
  experiment_id STRING
)
PARTITIONED BY(experiment_id)
;

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_chem_comp
(
  seq_id LONG,
  updated_ts TIMESTAMP NOT NULL,
  experiment_id STRING
)
PARTITIONED BY(experiment_id)
;

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_exptl
(
  seq_id LONG,
  updated_ts TIMESTAMP NOT NULL,
  experiment_id STRING
)
PARTITIONED BY(experiment_id)
;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SHOW DATABASES
