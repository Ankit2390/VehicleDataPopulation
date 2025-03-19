# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS fact_vehicle_registration (
# MAGIC     registration_id STRING,
# MAGIC     date_id INT,
# MAGIC     vin_id STRING,
# MAGIC     owner_location_id STRING,
# MAGIC     census_tract_id STRING,
# MAGIC     electric_range INT,
# MAGIC     base_msrp INT,
# MAGIC     legislative_district_id INT,
# MAGIC     registration_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/Provisioned/fact_vehicle_registration';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/user/hive/warehouse/Provisioned/fact_vehicle_registration`

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into fact_vehicle_registration
# MAGIC SELECT 
# MAGIC     MD5(CONCAT(fvr.vin, fvr.created_at)) AS registration_id,  -- Unique ID for each registration
# MAGIC     d.date_id,
# MAGIC     v.vin AS vin_id,
# MAGIC     MD5(CONCAT(l.state, l.county, l.city)) AS owner_location_id,  -- Unique location ID
# MAGIC     c.census_tract_id AS census_tract_id,
# MAGIC     fvr.electric_range,
# MAGIC     fvr.base_msrp,
# MAGIC     fvr.legislative_district AS legislative_district_id,
# MAGIC     from_unixtime(fvr.created_at) AS registration_timestamp
# MAGIC FROM  delta.`/user/hive/warehouse/electricvehicledata` fvr
# MAGIC LEFT JOIN delta.`/Provisioned/dim_date` d ON fvr.date_as_of_date = d.date_id
# MAGIC LEFT JOIN delta.`/Provisioned/DimVehicle` v ON fvr.vin = v.vin
# MAGIC LEFT JOIN delta.`/Provisioned/dim_location` l 
# MAGIC     ON MD5(CONCAT(fvr.state, fvr.county, fvr.city))= l.owner_location_id
# MAGIC LEFT JOIN delta.`/Provisioned/dim_census_tract` c ON fvr._2020_census_tract = c.census_tract_id
# MAGIC LEFT JOIN delta.`/Provisioned/dim_legislative_district` ld 
# MAGIC     ON fvr.legislative_district = ld.legislative_district_id;
