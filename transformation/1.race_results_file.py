# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## This notebook is going to generate a race_results file
# MAGIC The race_results file groups information from files on the processed layer: race, circuits, drivers and constructor

# COMMAND ----------

# MAGIC %run "/formula1/ingestion/includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Importing races file

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/race")\
    .select("race_id","race_year",col("name").alias("race_name"),col("race_timestamp").alias("race_date"),"circuit_id")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Circuits

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
    .select("circuit_id",col("name").alias("circuit_name"),col("location").alias("circuit_location"))

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importing drivers

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")\
    .select("driver_id",col("name").alias("driver_name"),col("number").alias("driver_number"),col("nationality").alias("driver_nationality"))

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Constructor file

# COMMAND ----------

constructor_df = spark.read.parquet(f"{processed_folder_path}/constructors")\
    .select(col("constructorid").alias("constructor_id"),col("name").alias("team"))

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import result_files

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results")\
    .select("result_id","race_id","driver_id","constructor_id","grid","fastest_lap",col("time").alias("race_time"),"points","position")


# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing results file

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join races_df and circuits_df

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df,circuits_df.circuit_id == races_df.circuit_id)\
    .select(races_df.race_id,races_df.race_year,races_df.race_name,races_df.race_date,circuits_df.circuit_location)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join with results

# COMMAND ----------

results_df_1 = results_df.join(race_circuits_df,results_df.race_id==race_circuits_df.race_id)\
                         .join(drivers_df,results_df.driver_id == drivers_df.driver_id)\
                         .join(constructor_df,results_df.constructor_id == constructor_df.constructor_id)

# COMMAND ----------

display(results_df_1)

# COMMAND ----------

final_df = results_df_1.select("race_name","race_year","race_date","circuit_location","driver_name","driver_number","driver_nationality"
                               ,"team","grid","fastest_lap","race_time","points","position")\
                       .withColumn("created_date",current_timestamp())

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Writing race_results file in presentation folder

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(spark.read.parquet(f"{presentation_folder_path}/race_results"))

# COMMAND ----------


