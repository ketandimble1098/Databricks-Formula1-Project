# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "/Workspace/Formula1/Includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,concat,lit
from pyspark.sql.types import StringType,StructField,StructType,IntegerType,DateType

# COMMAND ----------

# MAGIC %md
# MAGIC nested json file

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(),True),
    StructField("surname",StringType(),True)])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId",IntegerType(), False),
                                  StructField("driverRef",StringType(), True),
                                  StructField("number",IntegerType(), True),
                                  StructField("code",StringType(), True),
                                  StructField("name", name_schema),
                                  StructField("dob",DateType(), True),
                                  StructField("nationality",StringType(), True),
                                  StructField("url",StringType(), True)

])

# COMMAND ----------

# MAGIC %md
# MAGIC Rename columns and add new columns
# MAGIC - driverId renamed to driver_id
# MAGIC - driverRef renamed to driver_ref
# MAGIC - ingestion data added
# MAGIC - name added with concatenation of forename and surname

# COMMAND ----------

drivers_df = spark.read\
        .option("header",True)\
        .schema(drivers_schema)\
        .json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId","driver_id")\
                                    .withColumnRenamed("driverRef","driver_Ref")\
                                    .withColumn("ingestion_date", current_timestamp())\
                                    .withColumn("name",concat(col("name.forename"), lit(' '),col("name.surname")))\
                                    .withColumn("data_source",lit(v_data_source))\
                                    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC Drop the unwanted columns
# MAGIC - name.forename
# MAGIC - name.surname
# MAGIC - url

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop("url")

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

display(spark.read.format("delta").load(f"{processed_folder_path}/drivers"))

# COMMAND ----------

