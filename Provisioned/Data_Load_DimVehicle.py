# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS DimVehicle (
# MAGIC     VIN STRING,
# MAGIC     Model_Year INT,
# MAGIC     Make STRING,
# MAGIC     Model STRING,
# MAGIC     ev_type STRING,
# MAGIC     cafv_type STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'Provisioned/DimVehicle';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) FROM delta.`/Provisioned/DimVehicle`;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into DimVehicle
# MAGIC     select distinct
# MAGIC     vin,
# MAGIC     Model_Year,
# MAGIC     Make,
# MAGIC     Model,
# MAGIC     ev_type,
# MAGIC     cafv_type from delta.`/user/hive/warehouse/electricvehicledata`
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from delta.`/default/ElectricVehicleData`
