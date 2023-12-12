# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------



v_result_1 = dbutils.notebook.run("1.ingest_circuits_file",0,{"p_data_source":"Ergast API"})
print(v_result_1)

# COMMAND ----------

v_result_2 = dbutils.notebook.run("2-ingest_race_files",0,{"p_data_source":"Ergast API"})
print(v_result_2)

# COMMAND ----------

v_result_3 =  dbutils.notebook.run("3.ingesting_constructor_file",0, {"p_data_source":"Ergast API"})
print(v_result_3)

# COMMAND ----------

v_result_4 = dbutils.notebook.run("4.ingest_drivers_file",0, {"p_data_source":"Ergast API"})
print(v_result_4)

# COMMAND ----------

v_result_5 = dbutils.notebook.run("5.ingest_results_file",0, {"p_data_source":"Ergast API"})
print(v_result_5)


# COMMAND ----------

v_result_6 = dbutils.notebook.run("6.ingest_pit_stop",0, {"p_data_source":"Ergast API"})
print(v_result_6)

# COMMAND ----------

v_result_7 = dbutils.notebook.run("7.ingest_folder_lap_times",0, {"p_data_source":"Ergast API"})
print(v_result_7)

# COMMAND ----------

v_result_8 = dbutils.notebook.run("8.ingest_qualifying_files",0, {"p_data_source":"Ergast API"})
print(v_result_8)
