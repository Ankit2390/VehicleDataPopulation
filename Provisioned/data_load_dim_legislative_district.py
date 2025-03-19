# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dim_legislative_district (
# MAGIC     legislative_district_id INT,
# MAGIC     district_name STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/Provisioned/dim_legislative_district';

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dim_legislative_district 
# MAGIC     select distinct 
# MAGIC     legislative_district,
# MAGIC     'NA'as district_name  
# MAGIC    from delta.`/default/ElectricVehicleData`
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct county from delta.`/default/ElectricVehicleData`
