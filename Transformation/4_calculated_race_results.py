# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
    (
      race_year INT,
      team_name STRING,
      driver_id INT,
      driver_name STRING,
      race_id INT,
      position INT,
      points INT,
      calculated_points INT,
      created_date TIMESTAMP,
      updated_date TIMESTAMP
    ) USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    create or replace temp view race_results_updated AS
    select 
          races.race_year,
          constructors.name AS team_name,
          drivers.driver_id,
          drivers.name As driver_name,
          races.race_id,
          results.position,
          results.points,
          11 -results.position AS calculated_points
    from f1_processed.results
    join f1_processed.drivers on (results.driver_id = drivers.driver_id)
    join f1_processed.constructors on (results.constructor_id = constructors.constructor_id)
    join f1_processed.races on (results.race_id = races.race_id)
    where results.position <= 10 AND results.file_date = '{v_file_date}'
""")

# COMMAND ----------

spark.sql(f"""
MERGE INTO f1_presentation.calculated_race_results tgt
USING race_results_updated upd
ON tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id
WHEN MATCHED THEN
  UPDATE SET tgt.position = upd.position,
            tgt.points = upd.points,
            tgt.calculated_points = upd.calculated_points,
            tgt.updated_date = current_timestamp
WHEN NOT MATCHED
  THEN INSERT (
      race_year,
      team_name,
      driver_id,
      driver_name,
      race_id,
      position,
      points,
      created_date
  )
  VALUES (
    race_year,
    team_name,
    driver_id,
    driver_name,
    race_id,
    position,
    points,
    current_timestamp
  )""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) from race_results_updated

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from f1_presentation.calculated_race_results

# COMMAND ----------

