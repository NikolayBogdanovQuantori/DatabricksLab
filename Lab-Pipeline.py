# Databricks notebook source
# MAGIC %run ./Config-Script

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE LAB;

# COMMAND ----------

tables_in_use = check_tables_columns(TABLES_INFO)

# COMMAND ----------

experiments_rows = spark.sql("SELECT experiment_id, is_forced FROM config_pdb_actualizer").collect()
experiments_files: list[ExperimentFile] = []
for row in experiments_rows[:20]:
    experiments_files.append(ExperimentFile(row["experiment_id"], row["is_forced"]))

# COMMAND ----------

fetch_files(hostname='ftp.wwpdb.org', directory='pub/pdb/data/structures/divided/mmCIF', filestore_prefix='FileStore/Lab/', experiments_files=experiments_files)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE updated_experiments
# MAGIC (
# MAGIC   experiment_id STRING NOT NULL,
# MAGIC   run_ts TIMESTAMP NOT NULL,
# MAGIC   hash_sum STRING NOT NULL  
# MAGIC )

# COMMAND ----------

import time

start = time.time()
end = time.time()
print(type(end-start))

TIME_MEASUREMENTS = {
    "is_present_check": [],
    "dict_read": [],
    "adding_rows": [],
    "register_tv": [],
    "deleting": [],
    "inserting": [],
    "insert_updated": []
}

# COMMAND ----------

for j, experiment in enumerate(experiments_files):
    print(j, experiment.id)
    start = time.time()
    is_experiment_present = spark.table("register_pdb_actualizer").select("experiment_id")\
        .where(col("experiment_id") == f"{experiment.id}")\
        .where(col("hash_sum") == f"{experiment.hash_sum}")\
        .count() > 0
    if is_experiment_present is True and experiment.is_forced is False:
        print(f"experiment {experiment.id} already in DB, file skipped")
        continue
    end = time.time()
    TIME_MEASUREMENTS["is_present_check"].append(end-start)
    start = time.time()
    mmcif_dict = MMCIF2Dict(experiment.path)
    end = time.time()
    TIME_MEASUREMENTS["dict_read"].append(end-start)
    for current_table in tables_in_use:
        fill_table(current_table, mmcif_dict, experiment.id)
    start = time.time()
    spark.sql(f'INSERT INTO updated_experiments VALUES("{experiment.id}", current_timestamp(), "{experiment.hash_sum}")')
    end = time.time()
    TIME_MEASUREMENTS["insert_updated"].append(end-start)

# COMMAND ----------

import statistics

for k, v in TIME_MEASUREMENTS.items():
    print(k, statistics.mean(v))

# COMMAND ----------

spark.sql("""
    MERGE INTO register_pdb_actualizer a
    USING updated_experiments b
    ON a.experiment_id = b.experiment_id
    WHEN MATCHED THEN
      UPDATE SET run_ts = b.run_ts, hash_sum = b.hash_sum
    WHEN NOT MATCHED THEN INSERT (experiment_id, run_ts, hash_sum) VALUES (b.experiment_id, b.run_ts, b.hash_sum)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM register_pdb_actualizer order by run_ts desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_entity;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_pdbx_database_PDB_obs_spr;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_entity_poly_seq;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_chem_comp order by id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_exptl;
