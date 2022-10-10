-- Databricks notebook source
USE DATABASE LAB;

-- COMMAND ----------

SELECT COUNT(DISTINCT (experiment_id)), COUNT(experiment_id) FROM register_pdb_actualizer --order by updated_ts desc;

-- COMMAND ----------

SELECT 
  experiment_id,
  MAX(updated_ts) as max_ts,
  MIN(updated_ts) as min_ts
FROM bronze_entity
GROUP BY experiment_id
HAVING max_ts != min_ts

-- COMMAND ----------

SELECT 
  updated_ts,
  COUNT(experiment_id)
FROM(
  SELECT 
    experiment_id,
    MAX(updated_ts) as updated_ts
  FROM bronze_entity
  GROUP BY experiment_id
)
GROUP BY updated_ts
ORDER BY updated_ts desc


-- COMMAND ----------

SELECT * FROM register_pdb_actualizer order by updated_ts desc;

-- COMMAND ----------

SELECT * FROM bronze_entity order by updated_ts desc, experiment_id, id;

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
