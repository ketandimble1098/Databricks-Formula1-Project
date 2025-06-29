# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Pitstops.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "/Workspace/Formula1/Includes/configuration"

# COMMAND ----------

# MAGIC %run "/Workspace/Formula1/Includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,concat,lit
from pyspark.sql.types import StringType,StructField,StructType,IntegerType,DateType

# COMMAND ----------

pitstops_schema = StructType(fields=[StructField("raceId",IntegerType(), False),
                              StructField("driverId",IntegerType(), True),
                              StructField("stop",IntegerType(), True),
                              StructField("lap",IntegerType(),True),
                              StructField("time",StringType(),True),
                              StructField("duration",StringType(),True),
                              StructField("milliseconds",IntegerType(),True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC Multiline json file

# COMMAND ----------

pitstops_df = spark.read\
              .option("multiLine", "true")\
              .schema(pitstops_schema)\
              .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC Rename the column and add ingestion_date column

# COMMAND ----------

pitstops_final_df = pitstops_df.withColumnRenamed("raceId","race_id")\
                                    .withColumnRenamed("driverId","driver_id")\
                                    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(pitstops_final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# pitstops_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

dbutils.notebook.exit("Success")