# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times `folder`

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

lap_times_schema = StructType(fields=[StructField("raceId",IntegerType(), False),
                              StructField("driverId",IntegerType(), True),
                              StructField("lap",IntegerType(),True),
                              StructField("position",IntegerType(),True),
                              StructField("time",StringType(),True),
                              StructField("milliseconds",IntegerType(),True)
])

# COMMAND ----------

lap_times_df = spark.read\
              .schema(lap_times_schema)\
              .csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Rename the column and add ingestion_date column

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("raceId","race_id")\
                                    .withColumnRenamed("driverId","driver_id")\
                                    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(lap_times_final_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")