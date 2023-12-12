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

lap_times_schema = StructType(fields=[ StructField("raceId",IntegerType(),False),
                                      StructField("driverId", IntegerType(),False),
                                      StructField("lap",IntegerType(),False),
                                      StructField("position",IntegerType(),True),                                     
                                      StructField("time",StringType(),True),
                                      StructField("milliseconds",IntegerType(),True)

])

# COMMAND ----------

lap_times_df = spark.read\
    .schema(lap_times_schema)\
    .csv(f"{raw_folder_path}/lap_times/lap_times_split_*.csv")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step2 - Rename and insert columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df_1 = lap_times_df.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id") \
.withColumn("datasource",lit(v_data_source))

# COMMAND ----------

final_df = add_ingestion_date(final_df_1)

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step3 - Write pit_stop parquet file

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/lap_times"))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ##Formating the exit message

# COMMAND ----------

notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_name_2 = notebook_name.split(sep="/")[-1]
dbutils.notebook.exit(f"{notebook_name_2} finished with success!")
