# Databricks notebook source
from ftplib import FTP
import gzip
import shutil
import hashlib
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType
from Bio.PDB.MMCIF2Dict import MMCIF2Dict
from pyspark.sql.types import StructField, StructType, StringType, LongType
from  pyspark.sql.functions import lit, col

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

class ExperimentFile:
    def __init__(self, id, is_forced):
        self.id = id.upper()
        self.is_forced = is_forced
        self.path = None
        self.hash_sum = None
    
    def __str__(self):
        return f"{self.id}, {self.is_forced}, {self.path}, {self.hash_sum}"

# COMMAND ----------

def fetch_files(hostname, directory, filestore_prefix, experiments_files: list[ExperimentFile]):
    DFBS_FILESTORE_PREFIX = f'/dbfs/{filestore_prefix}'
    ftp = FTP(hostname)
    ftp.login()
    ftp.cwd(directory)
    for j, file in enumerate(experiments_files):
        lower_file_id = file.id.lower()
        sub_directory = lower_file_id[1:3]
        ftp.cwd(sub_directory)
        filename = f"{lower_file_id}.cif.gz"
        print(f"{j}: processing {filename} at {hostname}{ftp.pwd()}")
        # fetching *.gz file to dfbs
        with open(f'{DFBS_FILESTORE_PREFIX}{filename}', 'wb') as fp:
            ftp.retrbinary(f'RETR {filename}', fp.write)
        # unziping content to *.cif file
        with gzip.open(f'{DFBS_FILESTORE_PREFIX}{filename}', 'rb') as f_in:
            with open(f'{DFBS_FILESTORE_PREFIX}{lower_file_id}.cif', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        experiments_files[j].path = f'{DFBS_FILESTORE_PREFIX}{lower_file_id}.cif'
        # deleting archive
        dbutils.fs.rm(f'{filestore_prefix}{filename}')    
        ftp.cwd('..')
        # calculating hashsum
        with open(experiments_files[j].path, 'r') as f_in:
            file_content = f_in.read()
            experiments_files[j].hash_sum = hashlib.md5(file_content.encode()).hexdigest()        
    ftp.quit()

# COMMAND ----------

class TableDecsriber:            
    def __init__(self, table_name, columns_schema):
        self.name = table_name
        self.raw_columns_schema = columns_schema
        self.columns_schema = {}
        for key, value in columns_schema.items():
            self.columns_schema['_'.join(key.split('.')[1:])] = value
    
    def set_data(self, mmcif_dict):
        columns_dict = {}
        dataset_size = -1
        for column_name in self.raw_columns_schema.keys():
            column_values = mmcif_dict.get(column_name)
            if column_values is not None:
                if dataset_size == - 1:
                    dataset_size = len(column_values)
                if dataset_size != len(column_values):
                    raise ValueError("lengths of columns do not match!")
            columns_dict[column_name] = column_values
        #  
        for column_name, column_values in columns_dict.items():
            if column_values is None:
                columns_dict[column_name] = [None] * dataset_size
        #
        self.rows = [[None for x in range(len(self.raw_columns_schema.keys()))] for y in range(dataset_size)]
        column_index = 0
        for _, column_values in columns_dict.items():
            for row_index, value in enumerate(column_values):
                self.rows[row_index][column_index] = value
            column_index += 1
    
    def get_rows(self):
        return self.rows

    def get_raw_schema(self):
        result_schema_columns_list = [StructField(column_name, StringType(), True) for column_name in self.columns_schema.keys()]
        return StructType(result_schema_columns_list)
  
    def get_schema_dict(self):
        return self.columns_schema

# COMMAND ----------

import time

def fill_table(current_table: TableDecsriber, mmcif_dict, experiment_id):
    # adding data rows to current TableDescriber object:
    start = time.time()
    current_table.set_data(mmcif_dict)
    end = time.time()
    TIME_MEASUREMENTS["adding_rows"].append(end-start)
    # defining temp view with STRING columns
    start = time.time()
    temp_view_name = f"v_temp_{current_table.name}"
    raw_df = spark.createDataFrame(current_table.get_rows(), current_table.get_raw_schema())
    raw_df = raw_df.withColumn("experiment_id", lit(f"{experiment_id}"))
    raw_df.createOrReplaceTempView(temp_view_name)
    #raw_df.show(2)
    end = time.time()
    TIME_MEASUREMENTS["register_tv"].append(end-start)
    #
    start = time.time()
    spark.sql(f'DELETE FROM {current_table.name} WHERE experiment_id="{experiment_id}"')
    end = time.time()
    TIME_MEASUREMENTS["deleting"].append(end-start)
    #
    start = time.time()
    insert_query = f"INSERT INTO {current_table.name} (experiment_id, {', '.join(current_table.get_schema_dict().keys())})\n"
    insert_query += "SELECT \n \texperiment_id\n"
    for column_name, column_type in current_table.get_schema_dict().items():
        insert_query += f"\t,CAST(CASE {column_name} WHEN '?' THEN NULL WHEN '.' THEN NULL ELSE {column_name} END AS {column_type}) AS {column_name}\n"
    insert_query += f"FROM {temp_view_name};"
    #print(insert_query)
    spark.sql(insert_query)
    end = time.time()
    TIME_MEASUREMENTS["inserting"].append(end-start)
