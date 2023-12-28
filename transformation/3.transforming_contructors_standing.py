# Databricks notebook source
# MAGIC %md
# MAGIC ### 1.Importing mounted paths 

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ###2.Reading parquet file with the race_results with the 2020 results
# MAGIC The race_results file was gerated in previous step

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results").filter("race_year = 2020")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Creating a df with the drivers standings
# MAGIC The agregated column "wins" is defined from the position column

# COMMAND ----------

from pyspark.sql.functions import sum,when,count, col

constructor_standings_df = race_results_df \
    .groupBy("race_year","team")\
    .agg(sum("points").alias("total_points"),
         count(when(col("position") == 1,True)).alias("wins"))
    

# COMMAND ----------

display(constructor_standings_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Creating a window_spec to calculate the rank of drivers based on total of points and number of victories
# MAGIC "This clause defines how the rows will be grouped, sorted within the group, and which rows within a partition a function operates on."

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank,asc

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = constructor_standings_df.withColumn("rank",rank().over(constructor_rank_spec))

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Writing the final result as a parquet file in the presentation layer folder

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")

# COMMAND ----------

display(spark.read.parquet(f"{presentation_folder_path}/constructor_standings"))

# COMMAND ----------


