# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../ingestion/includes/configuration"

# COMMAND ----------

# MAGIC %run "../ingestion/includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Read the CSV file using the spark dataframe reader 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType,StringType,DoubleType
circuits_schema = StructType(fields=[StructField("circuitId",IntegerType(),False),
                                     StructField("circuitRef",StringType(),False),
                                     StructField("name",StringType(),False),
                                     StructField("location",StringType(),False),
                                     StructField("country",StringType(),False),
                                     StructField("lat",DoubleType(),False),
                                     StructField("lng",DoubleType(),False),
                                     StructField("alt",IntegerType(),False),
                                     StructField("url",StringType(),False)
])

# COMMAND ----------

circuits_df = spark.read\
    .option("header",True)\
    .schema(circuits_schema) \
    .csv(f"{raw_folder_path}/circuits.csv")

#display(circuits_df)
#circuits_df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC Rename Column

# COMMAND ----------

from pyspark.sql.functions import col,lit

circuits_selected_df = circuits_df.select(col('circuitId'),
 col('circuitRef'),
 col('name'),
 col('location'),
 col('country'),
 #col('country').alias("race_country"),
 col('lat'),
 col('lng'),
 col('alt'))


# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed('circuitId','circuit_id')\
.withColumnRenamed('circuitRef','circuit_ref')\
.withColumnRenamed('lat','latitude')\
.withColumnRenamed('lng','longitude')\
.withColumnRenamed('alt','altitude')\
.withColumn("datasource",lit(v_data_source))

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Inserting columns

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC step 5 - Write circuits parquet file

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1itassadl/processed/circuits

# COMMAND ----------

df = spark.read.parquet(f"{processed_folder_path}/circuits")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Formating the exit message

# COMMAND ----------

notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_name_2 = notebook_name.split(sep="/")[-1]

# COMMAND ----------

dbutils.notebook.exit(f"{notebook_name_2} finished with success!")

# COMMAND ----------


