-- Databricks notebook source
USE DATABASE LAB;

-- COMMAND ----------

SELECT * FROM register_pdb_actualizer order by updated_ts desc;

-- COMMAND ----------

SELECT * FROM bronze_entity order by experiment_id, id;

-- COMMAND ----------

SELECT * FROM silver_entity order by experiment_id, id;

-- COMMAND ----------

SELECT * FROM bronze_pdbx_database_PDB_obs_spr order by experiment_id, replace_pdb_id;

-- COMMAND ----------

SELECT * FROM silver_pdbx_database_PDB_obs_spr order by experiment_id, replace_pdb_id;

-- COMMAND ----------

SELECT * FROM bronze_entity_poly_seq order by experiment_id, entity_id, mon_id;

-- COMMAND ----------

SELECT * FROM silver_entity_poly_seq order by experiment_id, entity_id, mon_id;

-- COMMAND ----------

SELECT * FROM bronze_chem_comp order by experiment_id, id;

-- COMMAND ----------

SELECT * FROM silver_chem_comp order by experiment_id, id;

-- COMMAND ----------

SELECT * FROM bronze_exptl order by experiment_id;

-- COMMAND ----------

SELECT * FROM silver_exptl order by experiment_id;
