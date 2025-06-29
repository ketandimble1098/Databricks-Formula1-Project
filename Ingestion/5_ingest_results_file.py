# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest result.json file

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
from pyspark.sql.types import StringType,StructField,StructType,IntegerType,DateType,FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId",IntegerType(), False),
                                  StructField("raceId",IntegerType(), True),
                                  StructField("driverId",IntegerType(), True),
                                  StructField("constructorId",IntegerType(), True),
                                  StructField("number", IntegerType(),True),
                                  StructField("grid",IntegerType(), True),
                                  StructField("position",IntegerType(), True),
                                  StructField("positionText",StringType(), True),
                                  StructField("positionOrder",IntegerType(), True),
                                  StructField("points",FloatType(), True),
                                  StructField("laps",IntegerType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("milliseconds",IntegerType(), True),
                                  StructField("fastestLap",IntegerType(), True),
                                  StructField("rank",IntegerType(), True),
                                  StructField("fastestLapTime",StringType(), True),
                                  StructField("fastestLapSpeed",StringType(), True),
                                  StructField("statusId",IntegerType(), True)
])

# COMMAND ----------

results_df = spark.read\
            .schema(results_schema)\
            .json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC Renaming columns and add ingestion date

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId","result_id") \
                                    .withColumnRenamed("raceId","race_id") \
                                    .withColumnRenamed("driverId","driver_id") \
                                    .withColumnRenamed("constructorId","constructor_id") \
                                    .withColumnRenamed("positionText","position_text") \
                                    .withColumnRenamed("positionOrder","position_order") \
                                    .withColumnRenamed("fastestLap","fastest_lap") \
                                    .withColumnRenamed("fastestLapTime","fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                                    .withColumn("ingestion_date", current_timestamp())\
                                    .withColumn("data_source", lit(v_data_source))\
                                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC Drop unwanted columns

# COMMAND ----------

results_final_df = results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method1
# MAGIC For loop drop the duplicate data partitions

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER Table f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method2

# COMMAND ----------

# MAGIC %md
# MAGIC Drop Duplicates

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning.enabled","true")
# from delta.tables import DeltaTable
# if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     deltaTable = DeltaTable.forPath(spark, "/mnt/formula1car10dl/processed/results")
#     deltaTable.alias("tgt").merge(
#         results_final_df.alias("src"),
#         "tgt.result_id = src.result_id")\
#         .whenMatchedUpdateAll()\
#         .whenNotMatchedInsertAll()\
#         .execute()
# else:
#     results_final_df.write.mode("overwrite").partitionBy("result_id").format("delta").saveAsTable("f1_processed.results")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

