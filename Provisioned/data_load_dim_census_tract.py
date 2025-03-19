# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dim_census_tract (
# MAGIC     census_tract_id STRING,
# MAGIC     region STRING,
# MAGIC     state STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/Provisioned/dim_census_tract';

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dim_census_tract 
# MAGIC     select distinct 
# MAGIC     CONCAT_WS('_', State,County,zip_code) as census_tract_id,
# MAGIC     null as REGION,
# MAGIC     state from delta.`/default/ElectricVehicleData`
