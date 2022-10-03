# Databricks notebook source
from ftplib import FTP
import gzip
import shutil
import hashlib
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, TimestampType
from Bio.PDB.MMCIF2Dict import MMCIF2Dict
from pyspark.sql.types import StructField, StructType, StringType, LongType
from  pyspark.sql.functions import lit, col, current_timestamp
from datetime import datetime
#
import time

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
    
    def set_data(self, mmcif_dict, ):
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

    def get_raw_df_schema(self):
        result_schema_columns_list = [StructField(column_name, StringType(), True) for column_name in self.columns_schema.keys()]
        return StructType(result_schema_columns_list)
  
    def get_schema_dict(self):
        return self.columns_schema

# COMMAND ----------

def process_experiments(tables: list[TableDecsriber], experiments_files: list[ExperimentFile]):
    updated_experiments_schema = StructType([
        StructField('experiment_id', StringType(), False),
        StructField('updated_ts', TimestampType(), False),
        StructField('hash_sum', StringType(), False)
    ])
    updated_experiments_data: list[tuple[str, datetime.datetime, str]] = []
    #
    tables_dataframes = {}
    for table in tables:
        tables_dataframes[table.name] = spark.createDataFrame([], table.get_raw_df_schema())
        tables_dataframes[table.name] = tables_dataframes[table.name].withColumn('experiment_id', lit(None).cast(StringType()))
    #
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
        #
        start = time.time()
        mmcif_dict = MMCIF2Dict(experiment.path)
        end = time.time()
        TIME_MEASUREMENTS["dict_read"].append(end-start)
        #
        for table in tables:
            start = time.time()
            table.set_data(mmcif_dict)
            end = time.time()
            TIME_MEASUREMENTS["set_data_for_table"].append(end-start)
            #
            start = time.time()
            current_experiment_current_table_df = spark.createDataFrame(table.get_rows(), table.get_raw_df_schema())
            current_experiment_current_table_df = current_experiment_current_table_df.withColumn('experiment_id', lit(experiment.id).cast(StringType()))
            end = time.time()
            TIME_MEASUREMENTS["create_experiment_table_df"].append(end-start)
            #
            start = time.time()
            tables_dataframes[table.name] = tables_dataframes[table.name].union(current_experiment_current_table_df)
            end = time.time()
            TIME_MEASUREMENTS["union"].append(end-start)
        #
        start = time.time()
        updated_experiments_data.append(tuple((experiment.id, datetime.now(), experiment.hash_sum)))
        end = time.time()
        TIME_MEASUREMENTS["append_updated"].append(end-start)
    #
    for table in tables:
        df = tables_dataframes[f"{table.name}"]
        df.createOrReplaceTempView("temp_v_current_table_df")
        #
        start = time.time()
        delete_query = f"DELETE FROM {table.name} WHERE experiment_id IN (SELECT DISTINCT experiment_id FROM temp_v_current_table_df)"
        spark.sql(delete_query)
        end = time.time()
        TIME_MEASUREMENTS["deleting"].append(end-start)
        #
        start = time.time()
        insert_query = f"INSERT INTO {table.name} (experiment_id, updated_ts, {', '.join(table.get_schema_dict().keys())})\n"
        insert_query += "SELECT \n \texperiment_id,\n \tcurrent_timestamp() as updated_ts\n"
        for column_name in table.get_schema_dict().keys():
            insert_query += f"\t, {column_name}\n"
        insert_query += f"FROM temp_v_current_table_df;"
        spark.sql(insert_query)
        end = time.time()
        TIME_MEASUREMENTS["inserting"].append(end-start)
        print(f"{table.name} updated")
    #
    start = time.time()
    updated_experiments_df = spark.createDataFrame(updated_experiments_data, updated_experiments_schema)
    updated_experiments_df.createOrReplaceTempView("temp_v_updated_experiments_df")
    end = time.time()
    TIME_MEASUREMENTS["create_updated_experiment_temp_v"].append(end-start)
