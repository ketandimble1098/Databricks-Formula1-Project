-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:black; text-align:center; font-family:arial">Dominant F1 Teams</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace temp view v_dominant_teams as
select 
  team_name,
  count(1) AS total_races,
  sum(calculated_points) AS total_points,
  round(avg(calculated_points),2) As avg_points,
  rank() over (order by avg(calculated_points) desc) as team_rank
from f1_presentation.calculated_race_results
group by team_name
having count(1) >= 100
order by avg_points desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dominant teams every year

-- COMMAND ----------

select 
  race_year,
  team_name,
  count(1) AS total_races,
  sum(calculated_points) AS total_points,
  round(avg(calculated_points),2) As avg_points
from f1_presentation.calculated_race_results
where team_name in (select team_name from v_dominant_teams where team_rank <= 10)
group by race_year, team_name
order by race_year, avg_points desc

-- COMMAND ----------

select 
  race_year,
  team_name,
  count(1) AS total_races,
  sum(calculated_points) AS total_points,
  round(avg(calculated_points),2) As avg_points
from f1_presentation.calculated_race_results
where team_name in (select team_name from v_dominant_teams where team_rank <= 10)
group by race_year, team_name
order by race_year, avg_points desc

-- COMMAND ----------

