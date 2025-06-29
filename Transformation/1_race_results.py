# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "/Workspace/Formula1/Includes/configuration"

# COMMAND ----------

# MAGIC %run "/Workspace/Formula1/Includes/common_functions"

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits")\
    .withColumnRenamed("location", "circuit_location")

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races")\
    .withColumnRenamed("race_timestamp", "race_date")\
    .withColumnRenamed("name", "race_name")

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors")\
    .withColumnRenamed("name", "team")

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers")\
    .withColumnRenamed("name", "driver_name")\
    .withColumnRenamed("number", "driver_number")\
    .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results")\
    .filter(f"file_date = '{v_file_date}'")\
    .withColumnRenamed("time", "race_time")\
    .withColumnRenamed("race_id", "result_race_id")\
    .withColumnRenamed("file_date", "result_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC join circuit and races df

# COMMAND ----------

race_circuit_join_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner")\
    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC join other dataframes to results

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

results_join_df = results_df.join(race_circuit_join_df, results_df.result_race_id == race_circuit_join_df.race_id)\
.join(drivers_df, results_df.driver_id == drivers_df.driver_id)\
.join(constructors_df, results_df.constructor_id == constructors_df.constructor_Id)

# COMMAND ----------

results_final_df = results_join_df.select("race_id","race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points","position","result_file_date", current_timestamp().alias("created_date"))\
  .withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

# results_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

# overwrite_partition(results_final_df, "race_results", "f1_presentation", "race_id")

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(results_final_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM f1_presentation.race_results;

# COMMAND ----------

