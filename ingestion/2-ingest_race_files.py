# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step1 - Read file

# COMMAND ----------

# MAGIC %run "../ingestion/includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../ingestion/includes/common_functions"

# COMMAND ----------

race_df = spark.read.option("header",True).csv(f'{raw_folder_path}/races.csv')


# COMMAND ----------

race_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step2 - Changing schema and selecting columns

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType,StringType,DoubleType

# COMMAND ----------

race_schema = StructType(fields=[StructField("raceId", IntegerType(),False),
                                 StructField("year",IntegerType(),False),
                                 StructField("round",IntegerType(),False),
                                 StructField("circuitId",IntegerType(),False),
                                 StructField("name",StringType(),False),
                                 StructField("date",StringType(),False),
                                 StructField("time",StringType(),False),
                                 StructField("url",StringType(),False)
]
)

# COMMAND ----------

race_df = spark.read.option("header",True)\
    .schema(race_schema)\
    .csv(f'{raw_folder_path}/races.csv')

# COMMAND ----------

race_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
race_selected_df = race_df.select(
    col("raceId").alias("race_id"),
    col("year"),
    col("round"),
    col("circuitId"),
    col("name"),
    col("date"),
    col("time")
)

display(race_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Including new columns race_timestamp

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,concat,lit

race_selected_df2 = race_selected_df.withColumn("race_timestamp",
                                                to_timestamp(concat(col("date"),lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"))\
                                    .withColumn("data_source",lit(v_data_source))


display(race_selected_df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step4 - including column ingestion_date
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

race_selected_df3 = add_ingestion_date(race_selected_df2)

display(race_selected_df3)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Renaming columns

# COMMAND ----------

from pyspark.sql.functions import col
race_selected_df4 = race_selected_df3.select(col("race_id"),
                                             col("year").alias("race_year"),
                                             col("round"),
                                             col("circuitId").alias("circuit_id"),
                                             col("name"),
                                             col("race_timestamp"),
                                             col("ingestion_date"))
display(race_selected_df4)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Write Parquet

# COMMAND ----------

race_selected_df4.write.mode("overwrite").parquet(f"{processed_folder_path }/race")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step7-Including Partitions

# COMMAND ----------

race_selected_df4.write.mode("overwrite")\
    .partitionBy("race_year")\
    .parquet(f"{processed_folder_path}/race")

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/race/race_year=1986/'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Formating the exit message

# COMMAND ----------

notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_name_2 = notebook_name.split(sep="/")[-1]

# COMMAND ----------

dbutils.notebook.exit(f"{notebook_name_2} finished with success!")
