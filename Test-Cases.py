# Databricks notebook source
# MAGIC %sql
# MAGIC USE DATABASE LAB;

# COMMAND ----------

def fill_config_pdb_actualizer(path):
    with open(path, 'r') as infile:
        lines = infile.readlines()
    spark.sql("TRUNCATE TABLE config_pdb_actualizer")
    insert_sql = "INSERT INTO config_pdb_actualizer VALUES\n"
    values = [f'("{line.rstrip()}", TRUE)' for line in lines]
    insert_sql += ',\n'.join(values)
    spark.sql(insert_sql)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM config_pdb_actualizer order by experiment_id;

# COMMAND ----------

# %sql
# TRUNCATE TABLE config_pdb_actualizer;

# INSERT INTO config_pdb_actualizer VALUES
#     ("100d", FALSE),
#     --("1NIH", TRUE),
#     ("402d", TRUE)d
# ;

# COMMAND ----------

#fill_config_pdb_actualizer('/dbfs/FileStore/Lab/Tests/test_1.txt') # 307
#fill_config_pdb_actualizer('/dbfs/FileStore/Lab/Tests/insulin_experiments_list.txt') # 316
fill_config_pdb_actualizer('/dbfs/FileStore/Lab/Tests/green_fluorescent_protein_experiments_list.txt')V # 426

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM config_pdb_actualizer order by experiment_id;
