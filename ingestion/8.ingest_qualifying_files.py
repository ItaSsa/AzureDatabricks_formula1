# Databricks notebook source


# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../ingestion/includes/configuration"

# COMMAND ----------

# MAGIC %run "../ingestion/includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step1 - Read the file to a dataframe

# COMMAND ----------

from pyspark.sql.types import StructType, StructField,IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[ StructField("qualifyId",IntegerType(),False),
                                      StructField("raceId",IntegerType(),False),
                                      StructField("driverId", IntegerType(),False),
                                      StructField("constructorId ", IntegerType(),False),
                                      StructField("number",IntegerType(),False),
                                      StructField("position",IntegerType(),True),                                     
                                      StructField("q1",StringType(),True),
                                      StructField("q2",StringType(),True),
                                      StructField("q3",StringType(),True)

])

# COMMAND ----------

display(dbutils.fs.ls(f"{raw_folder_path}/qualifying"))

# COMMAND ----------

qualifying_df = spark.read\
    .schema(qualifying_schema)\
    .option("multiline", True)\
    .json(f"{raw_folder_path}/qualifying")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step2 - Rename and insert columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_df_1 = qualifying_df.withColumnRenamed("qualifyId","qualify_id")\
.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("constructorId","constructor_id") \
.withColumn("datasource",lit(v_data_source))

# COMMAND ----------

final_df = add_ingestion_date(final_df_1)

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step3 - Write pit_stop parquet file

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/qualifying"))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ##Formating the exit message

# COMMAND ----------

notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_name_2 = notebook_name.split(sep="/")[-1]
dbutils.notebook.exit(f"{notebook_name_2} finished with success!")
