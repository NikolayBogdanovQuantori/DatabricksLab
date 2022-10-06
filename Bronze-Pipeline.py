# Databricks notebook source
# MAGIC %run ./Config-Script

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE LAB;

# COMMAND ----------

tables_in_use = check_bronze_tables_columns(TABLES_INFO)

# COMMAND ----------

experiments_rows = spark.sql("SELECT experiment_id, is_forced FROM config_pdb_actualizer ORDER BY experiment_id").collect()
experiments_files: list[ExperimentFile] = []
for row in experiments_rows:
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
    "deleting_total": [],
    "inserting_total": []
}

process_experiments_to_bronze(tables_in_use, experiments_files, 200)

# COMMAND ----------

total = 0
for k, v in TIME_MEASUREMENTS.items():
    s = sum(v)
    n = len(v)
    if n == 0:
        n = 1
    if k.find("total") == -1:
        total += s
    print(f"{k} performed {n} times, average time: {s/n} seconds, total_time {s/60} minutes")
    
print(f"total: {total/60} minutes")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO register_pdb_actualizer a
# MAGIC USING global_temp.g_temp_v_updated_experiments_df b
# MAGIC ON a.experiment_id = b.experiment_id
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET updated_ts = b.updated_ts, hash_sum = b.hash_sum
# MAGIC WHEN NOT MATCHED THEN INSERT (experiment_id, updated_ts, hash_sum) VALUES (b.experiment_id, b.updated_ts, b.hash_sum)
