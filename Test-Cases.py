# Databricks notebook source
# MAGIC %sql
# MAGIC USE DATABASE LAB;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM config_pdb_actualizer;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE config_pdb_actualizer;
# MAGIC 
# MAGIC INSERT INTO config_pdb_actualizer VALUES
# MAGIC     ("100d", FALSE),
# MAGIC     --("1NIH", TRUE),
# MAGIC     ("402d", TRUE)d
# MAGIC ;

# COMMAND ----------

def fill_config_pdb_actualizer(path):
    with open(path, 'r') as infile:
        lines = infile.readlines()
    spark.sql("TRUNCATE TABLE config_pdb_actualizer")
    insert_sql = "INSERT INTO config_pdb_actualizer VALUES\n"
    values = [f'("{line.rstrip()}", TRUE)' for line in lines]
    insert_sql += ',\n'.join(values)
    spark.sql(insert_sql)
    

fill_config_pdb_actualizer('/dbfs/FileStore/Lab/Tests/test_1.txt')
