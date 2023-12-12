# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../ingestion/includes/configuration"

# COMMAND ----------

# MAGIC %run "../ingestion/includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step1-Read the JSON file using the spark dataframe reader API

# COMMAND ----------

display( spark.read\
    .option("header",True)\
    .option("inferSchema",True) \
    .json(f"{raw_folder_path}/drivers.json"))

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename",StringType(),True),
                                 StructField("surname", StringType(),True)
])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId",IntegerType(),False),
                                    StructField("driverRef",StringType(),False),
                                    StructField("number",IntegerType(),True),
                                    StructField("code",StringType(),False),
                                    StructField("name",name_schema),
                                    StructField("dob",DateType(),True),
                                    StructField("nationality",StringType(),True),
                                    StructField("url",StringType(),True)
])

# COMMAND ----------

drivers_df = spark.read\
    .schema(drivers_schema)\
    .json(f"{raw_folder_path}/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step2- Rename Columns and add new Column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col,concat,lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId","driver_id")\
                                    .withColumnRenamed("driverRef","driver_ref")\
                                    .withColumn("datasource",lit(v_data_source))\
                                    .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))

# COMMAND ----------

drivers_with_dataingestion_df = add_ingestion_date(drivers_with_columns_df)

# COMMAND ----------

display(drivers_with_dataingestion_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step3- Drop the unwanted columns

# COMMAND ----------

drivers_final_df =  drivers_with_dataingestion_df.drop(col("url"))

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4 - Write the processed file in parquet format 

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/drivers"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Formating the exit message

# COMMAND ----------

notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_name_2 = notebook_name.split(sep="/")[-1]
dbutils.notebook.exit(f"{notebook_name_2} finished with success!")
