# Databricks notebook source
# MAGIC %run ./Lab-Helper

# COMMAND ----------

TABLES_INFO ={
    "entity": {
        "_entity.id": "STRING",
        "_entity.details": "STRING",
        "_entity.formula_weight": "DOUBLE",
        "_entity.pdbx_description": "STRING",
        "_entity.pdbx_ec": "STRING",
        "_entity.type": "STRING",
        "_entity.pdbx_fragment": "STRING",
        "_entity.pdbx_mutation": "STRING",
        "_entity.pdbx_number_of_molecules": "INT",
        "_entity.src_method": "STRING"
    },
    "pdbx_database_PDB_obs_spr": {
        "_pdbx_database_PDB_obs_spr.pdb_id": "STRING",
        "_pdbx_database_PDB_obs_spr.replace_pdb_id": "STRING",
        "_pdbx_database_PDB_obs_spr.date": "TIMESTAMP",
        "_pdbx_database_PDB_obs_spr.details": "STRING",
        "_pdbx_database_PDB_obs_spr.id": "STRING"
    },
    "entity_poly_seq": {
        "_entity_poly_seq.entity_id": "STRING",
        "_entity_poly_seq.mon_id": "STRING",
        "_entity_poly_seq.num": "INT",
        "_entity_poly_seq.hetero": "STRING"
    },
    "chem_comp": {
        "_chem_comp.id": "STRING",
        "_chem_comp.formula": "STRING",
        "_chem_comp.formula_weight": "DOUBLE",
        "_chem_comp.mon_nstd_flag": "STRING",
        "_chem_comp.name": "STRING",
        "_chem_comp.pdbx_synonyms": "STRING",
        "_chem_comp.type": "STRING"
    },
    "exptl": {
        "_exptl.entry_id": "STRING",
        "_exptl.method": "STRING"
    }
}

# COMMAND ----------

def check_bronze_tables_columns(tables_info) -> list[TableDecsriber]:
    result_table_describers: list[TableDecsriber] = []
    for table_name, table_schema in tables_info.items():
        bronze_table_name = f"bronze_{table_name}"
        bronze_table_schema = {column_name: "STRING" for column_name in table_schema.keys()} # for bronze tables all column types are set to STRING
        result_table_describers.append(TableDecsriber(table_name=bronze_table_name, columns_schema=bronze_table_schema))
        columns_list = spark.table(bronze_table_name).columns
        for column_name in result_table_describers[-1].get_schema_dict().keys():
            if column_name not in columns_list:
                spark.sql(f"ALTER TABLE {bronze_table_name} ADD COLUMNS ({column_name} STRING);")
                print(f"new column {column_name} of type STRING was added to table {bronze_table_name}")
    return result_table_describers
