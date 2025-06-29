# Databricks notebook source
# MAGIC %md
# MAGIC ### Formula 1 constructor Standings

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "/Workspace/Formula1/Includes/configuration"

# COMMAND ----------

# MAGIC %run "/Workspace/Formula1/Includes/common_functions"

# COMMAND ----------

race_results_df = spark.read.format('delta').load(f"{presentation_folder_path}/race_results")\
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

from pyspark.sql.functions import sum,when,col,count

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

race_results_df = spark.read.format('delta').load(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_year_list))


# COMMAND ----------

constructor_standings_df = race_results_df \
    .groupBy('race_year','team') \
    .agg(sum('points').alias('total_points'),
        count(when(col('position') == 1, True)).alias('wins'))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

constructor_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
constructor_ranked_df = constructor_standings_df.withColumn('rank', rank().over(constructor_rank_spec))


# COMMAND ----------

# constructor_ranked_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.constructor_standings')

# COMMAND ----------

# overwrite_partition(constructor_ranked_df, 'constructor_standings','f1_presentation', 'race_year')

# COMMAND ----------

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta_data(constructor_ranked_df, 'f1_presentation', 'constructor_standings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.constructor_standings;

# COMMAND ----------

