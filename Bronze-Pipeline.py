# Databricks notebook source
# MAGIC %run ./Config-Script

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE LAB;

# COMMAND ----------

tables_in_use = check_bronze_tables_columns(TABLES_INFO)

# COMMAND ----------

experiments_rows = spark.sql("SELECT experiment_id, is_forced FROM config_pdb_actualizer order by experiment_id").collect()
experiments_files: list[ExperimentFile] = []
for row in experiments_rows[:5]:
    experiments_files.append(ExperimentFile(row["experiment_id"], row["is_forced"]))

# COMMAND ----------

fetch_files(hostname='ftp.wwpdb.org', directory='pub/pdb/data/structures/divided/mmCIF', filestore_prefix='FileStore/Lab/', experiments_files=experiments_files)

# COMMAND ----------

TIME_MEASUREMENTS = {
    "is_present_check": [],
    "dict_read": [],
    "set_data_for_table": [],
    "create_experiment_table_df": [],
    "union": [],
    "deleting": [],
    "inserting": [],
    "append_updated": [],
    "create_updated_experiment_temp_v": []
}

# COMMAND ----------

process_experiments(tables_in_use, experiments_files)

# COMMAND ----------

total = 0
for k, v in TIME_MEASUREMENTS.items():
    s = sum(v)
    n = len(v)
    total += s
    print(f"{k} performed {n} times, average time: {s/n} seconds, total_time {s/60} minutes")
    
print(f"total: {total/60} minutes")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO register_pdb_actualizer a
# MAGIC USING temp_v_updated_experiments_df b
# MAGIC ON a.experiment_id = b.experiment_id
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET updated_ts = b.updated_ts, hash_sum = b.hash_sum
# MAGIC WHEN NOT MATCHED THEN INSERT (experiment_id, updated_ts, hash_sum) VALUES (b.experiment_id, b.updated_ts, b.hash_sum)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM register_pdb_actualizer order by updated_ts desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_entity order by experiment_id, id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_pdbx_database_PDB_obs_spr order by experiment_id, replace_pdb_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_entity_poly_seq order by experiment_id, entity_id, mon_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_chem_comp order by experiment_id, id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_exptl order by experiment_id;
