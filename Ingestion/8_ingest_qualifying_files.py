# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying json files

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

qualifying_schema = StructType(fields=[StructField("qualifyId",IntegerType(), False),
                              StructField("raceId",IntegerType(), True),
                              StructField("driverId",IntegerType(), True),
                              StructField("constructorId",IntegerType(),True),
                              StructField("number",IntegerType(),True),
                              StructField("position",IntegerType(),True),
                              StructField("q1",StringType(),True),
                              StructField("q2",StringType(),True),
                              StructField("q3",StringType(),True)
])

# COMMAND ----------

qualifying_df = spark.read\
              .option("multiLine", "true")\
              .schema(qualifying_schema)\
              .json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

qualifying_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Rename the column and add ingestion_date column

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed("qualifyId","qualify_id")\
                                    .withColumnRenamed("raceId","race_id")\
                                    .withColumnRenamed("driverId","driver_id")\
                                    .withColumnRenamed("constructorId","constructor_id")\
                                    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(qualifying_final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")