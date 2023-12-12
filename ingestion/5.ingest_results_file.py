# Databricks notebook source
# MAGIC %md
# MAGIC ##Step1-Read File

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../ingestion/includes/configuration"

# COMMAND ----------

# MAGIC %run "../ingestion/includes/common_functions"

# COMMAND ----------

from  pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

# COMMAND ----------

result_schema = StructType(fields=[     StructField("resultId",IntegerType(),False),
                                        StructField("raceId",IntegerType(),False),
                                        StructField("driverId",IntegerType(),False),
                                        StructField("constructorId",IntegerType(),False),
                                        StructField("number",IntegerType(),True),
                                        StructField("grid",IntegerType(),False),
                                        StructField("position",IntegerType(),True),
                                        StructField("positionText",StringType(),False),
                                        StructField("positionOrder",IntegerType(),False),
                                        StructField("points",DoubleType(),False),   
                                        StructField("laps",IntegerType(),False),
                                        StructField("time",StringType(),True),
                                        StructField("milliseconds",IntegerType(),True),
                                        StructField("fastestLap",IntegerType(),True),  
                                        StructField("rank",IntegerType(),True),
                                        StructField("fastestLapTime",StringType(),True),
                                        StructField("fastestLapSpeed",StringType(),True), 
                                        StructField("statusId",IntegerType(),False )
])

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1itassadl/raw

# COMMAND ----------

results_df = spark.read\
            .schema(result_schema)\
            .json(f"{raw_folder_path}/results.json")

# COMMAND ----------

display(results_df.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step2 - Remove Unwanted Columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_drop_df = results_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step3 - Rename columns and add new

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

results_with_columns_df_1 = results_drop_df.withColumnRenamed("resultId","result_id")\
                                         .withColumnRenamed("raceId","race_id")\
                                         .withColumnRenamed("driverId","driver_id")\
                                         .withColumnRenamed("constructorId","constructor_id")\
                                         .withColumnRenamed("positionText","position_text")\
                                         .withColumnRenamed("positionOrder","position_order")\
                                         .withColumnRenamed("fastestLap","fastest_lap")\
                                         .withColumnRenamed("fastestLapTime","fastest_lap_time")\
                                         .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                                         .withColumn("datasource",lit(v_data_source))      

# COMMAND ----------

results_with_columns_df = add_ingestion_date(results_with_columns_df_1)

# COMMAND ----------

display(results_with_columns_df.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step4- Write the parquet file

# COMMAND ----------

results_with_columns_df.write.partitionBy("race_id").mode("overwrite").parquet(f"{processed_folder_path}/results")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1itassadl/processed/results/race_id=18

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/results/race_id=18"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Formating the exit message

# COMMAND ----------

notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_name_2 = notebook_name.split(sep="/")[-1]
dbutils.notebook.exit(f"{notebook_name_2} finished with success!")
