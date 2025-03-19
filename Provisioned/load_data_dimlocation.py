# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dim_location (
# MAGIC     owner_location_id STRING,
# MAGIC     county STRING,
# MAGIC     city STRING,
# MAGIC     state STRING,
# MAGIC     zip_code STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/Provisioned/dim_location';

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dim_location 
# MAGIC select distinct
# MAGIC     CONCAT_WS('_', State, County, City,zip_code) as owner_location_id,
# MAGIC     county,
# MAGIC     city,
# MAGIC     state,
# MAGIC     zip_code
# MAGIC    from delta.`/default/ElectricVehicleData`
