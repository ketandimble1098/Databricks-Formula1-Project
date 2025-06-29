-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION '/mnt/formula1car10dl/processed'

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION '/mnt/formula1car10dl/presentation'