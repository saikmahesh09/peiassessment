# Databricks notebook source
# MAGIC %sql
# MAGIC DROP CATALOG IF EXISTS raw CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP CATALOG IF EXISTS refine CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS raw;
# MAGIC CREATE CATALOG IF NOT EXISTS refine;

# COMMAND ----------

dbutils.fs.ls("/Workspace/Shared/PEIAssesment/src/raw/dataset")

# COMMAND ----------

# MAGIC %skip
# MAGIC import os
# MAGIC from pyspark.sql.functions import col, explode_outer
# MAGIC
# MAGIC def list_files_recursive(path: str):
# MAGIC     all_files = []
# MAGIC     try:
# MAGIC         for item in dbutils.fs.ls(path):
# MAGIC             if item.isDir():
# MAGIC                 all_files.extend(list_files_recursive(item.path))
# MAGIC             else:
# MAGIC                 all_files.append(item.path)
# MAGIC     except Exception:
# MAGIC         pass
# MAGIC     return all_files
# MAGIC
# MAGIC def process_and_write_dataset(base_dataset_path: str, target_database: str = "raw.default"):
# MAGIC     if not base_dataset_path.startswith("dbfs:/"):
# MAGIC         base_dataset_path = "dbfs:/" + base_dataset_path.lstrip("/")
# MAGIC         
# MAGIC     file_paths = list_files_recursive(base_dataset_path)
# MAGIC     if not file_paths:
# MAGIC         raise FileNotFoundError(f"Directory not found or empty: {base_dataset_path}")
# MAGIC         
# MAGIC     spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_database}")
# MAGIC     written_tables = {}
# MAGIC     
# MAGIC     for file_path in file_paths:
# MAGIC         file_name = file_path.split('/')[-1]
# MAGIC         if file_name.startswith('.'):
# MAGIC             continue
# MAGIC             
# MAGIC         rel_path = file_path.replace(base_dataset_path, "").lstrip("/")
# MAGIC         file_stem = os.path.splitext(file_name)[0].lower().replace("-", "_").replace(" ", "_")
# MAGIC         full_table_name = f"{target_database}.{file_stem}"
# MAGIC         
# MAGIC         try:
# MAGIC             df = parse_file(file_path)
# MAGIC             (df.write
# MAGIC                .format("delta")
# MAGIC                .mode("append")
# MAGIC                .option("mergeSchema", "true")
# MAGIC                .saveAsTable(full_table_name))
# MAGIC             written_tables[rel_path or file_name] = full_table_name
# MAGIC         except Exception as e:
# MAGIC             print(f"Error processing {file_name}: {str(e)}")
# MAGIC             
# MAGIC     return written_tables
# MAGIC
# MAGIC def parse_file(file_path: str):
# MAGIC     ext = file_path.split('.')[-1].lower()
# MAGIC     if ext == 'csv':
# MAGIC         return parse_csv(file_path)
# MAGIC     elif ext in ['xls', 'xlsx']:
# MAGIC         return parse_excel(file_path)
# MAGIC     elif ext == 'json':
# MAGIC         return parse_json_with_fanout(file_path)
# MAGIC     else:
# MAGIC         raise ValueError(f"Unsupported file extension: .{ext}")
# MAGIC
# MAGIC def parse_csv(file_path: str):
# MAGIC     return (spark.read
# MAGIC             .option("header", "true")
# MAGIC             .option("inferSchema", "true")
# MAGIC             .option("sep", ",")
# MAGIC             .csv(file_path))
# MAGIC
# MAGIC def parse_excel(file_path: str):
# MAGIC     return (spark.read
# MAGIC             .format("excel")
# MAGIC             .option("header", "true")
# MAGIC             .option("inferSchema", "true")
# MAGIC             .load(file_path))
# MAGIC
# MAGIC def parse_json_with_fanout(file_path: str):
# MAGIC     df = spark.read.option("multiline", "true").json(file_path)
# MAGIC     for field in df.schema.fields:
# MAGIC         if field.dataType.typeName() == "array":
# MAGIC             df = df.withColumn(field.name, explode_outer(col(field.name)))
# MAGIC     return df
# MAGIC
# MAGIC input_dataset_path = "Workspace/Shared/PEIAssesment/src/raw/dataset/"
# MAGIC process_and_write_dataset(input_dataset_path, target_database="raw.default")

# COMMAND ----------

"""Wanted to have a very light weight parser considering each file is of different types.
The same parser can be extended with autoloader or native connectors in case of database source to fetch and process data with minimal modifications.
Notice certain file level and data level issues so I have added strip and replace fucntions for easy conversions and for uniformity lower case was used. EX: Column_ID or Column ID >> column_id"""

# COMMAND ----------


import os
from pyspark.sql.functions import col, explode_outer

def list_files_recursive(path: str):
    all_files = []
    try:
        for item in dbutils.fs.ls(path):
            if item.isDir():
                all_files.extend(list_files_recursive(item.path))
            else:
                all_files.append(item.path)
    except Exception:
        pass
    return all_files

def process_and_write_dataset(base_dataset_path: str, target_database: str = "raw.default"):
    if not base_dataset_path.startswith("dbfs:/"):
        base_dataset_path = "dbfs:/" + base_dataset_path.lstrip("/")
        
    file_paths = list_files_recursive(base_dataset_path)
    if not file_paths:
        raise FileNotFoundError(f"Directory not found or empty: {base_dataset_path}")
        
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_database}")
    written_tables = {}
    
    for file_path in file_paths:
        file_name = file_path.split('/')[-1]
        if file_name.startswith('.'):
            continue
            
        rel_path = file_path.replace(base_dataset_path, "").lstrip("/")
        file_stem = os.path.splitext(file_name)[0].lower().replace("-", "_").replace(" ", "_") #splitextensionfortablename
        full_table_name = f"{target_database}.{file_stem}"
        
        try:
            df = parse_file(file_path)
            
            # removing spacing in column names and convert them to lowercase so whole code has standardised
            for c in df.columns:
                clean_col = c.strip().lower().replace(" ", "_").replace("-", "_").replace("(", "").replace(")", "").replace(".", "_")
                df = df.withColumnRenamed(c, clean_col)
                
            (df.write
               .format("delta")
               .mode("overwrite")
               .option("mergeSchema", "true")
               .saveAsTable(full_table_name))
            written_tables[rel_path or file_name] = full_table_name
        except Exception as e:
            print(f"Error processing {file_name}: {str(e)}")
            
    return written_tables

def parse_file(file_path: str):
    ext = file_path.split('.')[-1].lower()
    if ext == 'csv':
        return parse_csv(file_path)
    elif ext in ['xls', 'xlsx']:
        return parse_excel(file_path)
    elif ext == 'json':
        return parse_json_with_fanout(file_path)
    else:
        raise ValueError(f"Unsupported file extension: .{ext}")

def parse_csv(file_path: str):
    return (spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("sep", ",")
            .csv(file_path))

def parse_excel(file_path: str):
    return (spark.read
            .format("excel")
            .option("headerRows", 1)
            .option("inferSchema", "true")
            .load(file_path))

def parse_json_with_fanout(file_path: str):
    df = spark.read.option("multiline", "true").json(file_path)
    for field in df.schema.fields:
        if field.dataType.typeName() == "array":
            df = df.withColumn(field.name, explode_outer(col(field.name)))
    return df

input_dataset_path = "Workspace/Shared/PEIAssesment/src/raw/dataset/"
process_and_write_dataset(input_dataset_path, target_database="raw.default")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw.default.customer