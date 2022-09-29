# Databricks notebook source
# MAGIC %run ./Lab-Helper

# COMMAND ----------

TABLES_INFO ={
    "bronze_entity": {
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
    "bronze_pdbx_database_PDB_obs_spr": {
        "_pdbx_database_PDB_obs_spr.pdb_id": "STRING",
        "_pdbx_database_PDB_obs_spr.replace_pdb_id": "STRING",
        "_pdbx_database_PDB_obs_spr.date": "TIMESTAMP",
        "_pdbx_database_PDB_obs_spr.details": "STRING",
        "_pdbx_database_PDB_obs_spr.id": "STRING" 
    },
    "bronze_entity_poly_seq": {
        "_entity_poly_seq.entity_id": "STRING",
        "_entity_poly_seq.mon_id": "STRING",
        "_entity_poly_seq.num": "INT",
        "_entity_poly_seq.hetero": "STRING"
    },
    "bronze_chem_comp": {
        "_chem_comp.id": "STRING",
        "_chem_comp.formula": "STRING",
        "_chem_comp.formula_weight": "DOUBLE",
        "_chem_comp.mon_nstd_flag": "STRING",
        "_chem_comp.name": "STRING",
        "_chem_comp.pdbx_synonyms": "STRING",
        "_chem_comp.type": "STRING"
    },
    "bronze_exptl": {
        "_exptl.entry_id": "STRING",
        "_exptl.method": "STRING"
    }
}

# COMMAND ----------

def check_tables_columns(tables_info) -> list[TableDecsriber]:
    result_table_describers: list[TableDecsriber] = []
    for table_name, table_schema in tables_info.items():
        result_table_describers.append(TableDecsriber(table_name=table_name, columns_schema=table_schema))
        columns_list = spark.table(table_name).columns
        for column_name, column_type in result_table_describers[-1].get_schema_dict().items():
            if column_name not in columns_list:
                spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({column_name} {column_type});")
                print(f"new column {column_name} of type {column_type} was added to table {table_name}")
    return result_table_describers
