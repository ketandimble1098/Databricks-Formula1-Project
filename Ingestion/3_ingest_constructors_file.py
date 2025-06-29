# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "/Workspace/Formula1/Includes/configuration"

# COMMAND ----------

constructors_schema = "constructorId INT,constructorRef STRING,name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read\
        .schema(constructors_schema)\
        .json(f'{raw_folder_path}/{v_file_date}/constructors.json')

# COMMAND ----------

from pyspark.sql.functions import col,current_date,current_timestamp,lit

# COMMAND ----------

constructors_dropped_df = constructors_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC rename columns and add ingestion date

# COMMAND ----------

constructors_final_df = constructors_dropped_df.withColumnRenamed("constructorId","constructor_Id")\
                            .withColumnRenamed("constructorRef","constructor_Ref")\
                            .withColumn("ingestion_date", current_timestamp())\
                            .withColumn("data_source", lit(v_data_source))\
                            .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC write output to parquet file

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

display(spark.read.format("delta").load(f"{processed_folder_path}/constructors"))

# COMMAND ----------

