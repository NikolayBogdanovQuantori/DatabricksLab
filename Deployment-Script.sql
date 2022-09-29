-- Databricks notebook source
DROP DATABASE IF EXISTS LAB CASCADE;

CREATE DATABASE IF NOT EXISTS LAB;
USE DATABASE LAB;

-- COMMAND ----------

CREATE OR REPLACE TABLE config_pdb_actualizer
(
  experiment_id STRING,
  is_forced BOOLEAN
)
;

-- COMMAND ----------

CREATE OR REPLACE TABLE register_pdb_actualizer
(
  experiment_id STRING NOT NULL,
  run_ts TIMESTAMP NOT NULL,
  hash_sum STRING NOT NULL  
)

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze_entity
(
  seq_id LONG GENERATED ALWAYS AS IDENTITY,
  experiment_id STRING
)
PARTITIONED BY(experiment_id)
;


-- COMMAND ----------

CREATE OR REPLACE TABLE bronze_pdbx_database_PDB_obs_spr
(
  seq_id LONG GENERATED ALWAYS AS IDENTITY,
  experiment_id STRING
)
PARTITIONED BY(experiment_id)
;

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze_entity_poly_seq
(
  seq_id LONG GENERATED ALWAYS AS IDENTITY,
  experiment_id STRING
)
PARTITIONED BY(experiment_id)
;

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze_chem_comp
(
  seq_id LONG GENERATED ALWAYS AS IDENTITY,
  experiment_id STRING
)
PARTITIONED BY(experiment_id)
;

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze_exptl
(
  seq_id LONG GENERATED ALWAYS AS IDENTITY,
  experiment_id STRING
)
PARTITIONED BY(experiment_id)
;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SHOW DATABASES
