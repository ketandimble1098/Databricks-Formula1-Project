-- Databricks notebook source
select 
  driver_name,
  count(1) AS total_races,
  sum(calculated_points) AS total_points,
  round(avg(calculated_points),2) As avg_points
from f1_presentation.calculated_race_results
group by driver_name
having count(1) >= 50
order by avg_points desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Dominant drivers between year 2011 and 2020

-- COMMAND ----------

select 
  driver_name,
  count(1) AS total_races,
  sum(calculated_points) AS total_points,
  round(avg(calculated_points),2) As avg_points
from f1_presentation.calculated_race_results
where race_year between 2011 and 2020
group by driver_name
having count(1) >= 50
order by avg_points desc

-- COMMAND ----------

