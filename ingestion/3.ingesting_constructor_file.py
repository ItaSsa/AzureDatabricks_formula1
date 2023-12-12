# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../ingestion/includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../ingestion/includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step1 - Read the Jason file using the spark dataframe reader

# COMMAND ----------

# Define the schema using Hive pattern
constructor_schema =  "constructorId INT , constructorRef STRING, name STRING, nationality STRING, url STRING "

# COMMAND ----------

constructor_df =  spark.read\
    .schema(constructor_schema)\
    .json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step2 - Drop Unwanted Columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

contructor_dropped_df = constructor_df.drop(col("url"))

# COMMAND ----------

display(contructor_dropped_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step3 - Rename Columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

constructor_renamed_df = contructor_dropped_df.withColumnRenamed("contructorId","contructor_id")\
                                            .withColumnRenamed("constructorRef","constructor_ref")\
                                            .withColumn("ingestion_date",current_timestamp())\
                                            .withColumn("datasource",lit(v_data_source))

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_renamed_df)

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step4- Write output into parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/constructors"))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ##Formating the exit message

# COMMAND ----------

notebook_name = dbutils.notebook\
    .entry_point\
    .getDbutils()\
    .notebook()\
    .getContext()\
    .notebookPath()\
    .get()
    
notebook_name_2 = notebook_name.split(sep="/")[-1]

# COMMAND ----------

dbutils.notebook.exit(f"{notebook_name_2} finished with success!")
