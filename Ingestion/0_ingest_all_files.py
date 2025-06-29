# Databricks notebook source
v_result = dbutils.notebook.run("1_ingest_circuit_file", 0,{"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result = dbutils.notebook.run("2_ingest_races_file", 0,{"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})


# COMMAND ----------

v_result = dbutils.notebook.run("3_ingest_constructors_file", 0,{"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})


# COMMAND ----------

v_result = dbutils.notebook.run("4_ingest_drivers_file", 0,{"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})


# COMMAND ----------

v_result = dbutils.notebook.run("5_ingest_results_file", 0,{"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})


# COMMAND ----------

v_result = dbutils.notebook.run("6_ingest_Pitstops_file", 0,{"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})


# COMMAND ----------

v_result = dbutils.notebook.run("7_ingest_lap_time_files", 0,{"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})


# COMMAND ----------

v_result = dbutils.notebook.run("8_ingest_qualifying_files", 0,{"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

