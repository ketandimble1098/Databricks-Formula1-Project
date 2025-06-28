-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

Drop Table if exists f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits (
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
) USING csv
OPTIONS (path "/mnt/formula1car10dl/raw/circuits.csv", header true)

-- COMMAND ----------

select * from f1_raw.circuits;

-- COMMAND ----------

Drop Table if exists f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races (
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
) USING csv
OPTIONS (path "/mnt/formula1car10dl/raw/races.csv", header true)

-- COMMAND ----------

select * from f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Constructor table
-- MAGIC - Single Line JSON

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors (
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
) USING json
OPTIONS (path "/mnt/formula1car10dl/raw/constructors.json")

-- COMMAND ----------

select * from f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create drivers table
-- MAGIC - Single line json

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers (
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
) USING json
OPTIONS (path "/mnt/formula1car10dl/raw/drivers.json")

-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create results table
-- MAGIC - single line json

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results (
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points FLOAT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed FLOAT,
  statusId INT
) USING json
OPTIONS (path "/mnt/formula1car10dl/raw/results.json");

-- COMMAND ----------

select * from f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create pit stops table
-- MAGIC - Multi Line JSON

-- COMMAND ----------

Drop Table if exists f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops (
  driverId INT,
  duration STRING,
  lap INT,
  milliseconds INT,
  raceId INT,
  stop INT,
  time STRING
) USING JSON
OPTIONS (path "/mnt/formula1car10dl/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create lap times tables
-- MAGIC - multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "/mnt/formula1car10dl/raw/lap_times")

-- COMMAND ----------

select * from f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Qualifying table
-- MAGIC multiple files with multiline json

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT)
USING json
OPTIONS (path "/mnt/formula1car10dl/raw/qualifying", multiLine true)

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------

