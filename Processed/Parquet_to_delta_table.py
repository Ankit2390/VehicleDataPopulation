# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, lit
from delta.tables import DeltaTable

# Load Parquet data
parquet_path = "dbfs:/FileStore/tables/Test/Flatten_Vehicle_data.parquet"
df = spark.read.parquet(parquet_path)
df.createOrReplaceTempView("electric_vehicles")

# Run SQL query
result = spark.sql("SELECT * FROM electric_vehicles")
result.printSchema()
result.display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, lit
from delta.tables import DeltaTable

# Initialize Spark session with Delta support
spark = SparkSession.builder \
    .appName("DataQualityCheck") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    .getOrCreate()

# Load Parquet data
parquet_path = "dbfs:/FileStore/tables/Test/Flatten_Vehicle_data.parquet"
df = spark.read.parquet(parquet_path)

column_mapping = {
    ":sid": "sid",
    ":id": "id",
    ":position": "position",
    ":created_at": "created_at",
    ":created_meta": "created_meta",
    ":updated_at": "updated_at",
    ":updated_meta": "updated_meta",
    ":meta": "meta",
    "vin_1_10": "vin",
    "county": "county",
    "city": "city",
    "state": "state",
    "zip_code": "zip_code",
    "model_year": "model_year",
    "make": "make",
    "model": "model",
    "ev_type": "ev_type",
    "cafv_type": "cafv_type",
    "electric_range": "electric_range",
    "base_msrp": "base_msrp",
    "legislative_district": "legislative_district",
    "dol_vehicle_id": "dol_vehicle_id",
    "geocoded_column": "geocoded_column",
    "electric_utility": "electric_utility",
    "_2020_census_tract": "census_tract",
    ":@computed_region_x4ys_rtnd": "computed_region_x4ys_rtnd",
    ":@computed_region_fny7_vc3j": "computed_region_fny7_vc3j",
    ":@computed_region_8ddd_yn5v": "computed_region_8ddd_yn5v",
    "date_as_of_date": "date_as_of_date"
}

# List of ID columns to check for null values
id_columns = ["vin_1_10", "DOL_Vehicle_ID"]

# Perform null value check only for ID columns
null_counts = df.select([count(when(col(c).isNull() | isnan(col(c)), c)).alias(c) for c in id_columns])
null_counts.show(truncate=False)

# Add 'is_valid_record' column (True if no nulls in ID columns, False otherwise)
df = df.withColumn("is_valid_record", ~col("VIN_1_10").isNull() & ~col("DOL_Vehicle_ID").isNull())

for old_col, new_col in column_mapping.items():
    df = df.withColumnRenamed(old_col, new_col)
    
# Define Delta table path
delta_table_path = "/user/hive/warehouse/electricvehicledata"

df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(delta_table_path)
print(f"Data successfully written to new Delta table at {delta_table_path}")


# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ElectricVehicleData (
# MAGIC  sid string,
# MAGIC id string,
# MAGIC position string,
# MAGIC created_at string,
# MAGIC created_meta string,
# MAGIC updated_at string,
# MAGIC updated_meta string,
# MAGIC  meta string,
# MAGIC  vin string,
# MAGIC  county string,
# MAGIC  city string,
# MAGIC  state string,
# MAGIC  zip_code string,
# MAGIC  model_year string,
# MAGIC  make string,
# MAGIC  model string,
# MAGIC  ev_type string,
# MAGIC  cafv_type string,
# MAGIC  electric_range string ,
# MAGIC  base_msrp string,
# MAGIC  legislative_district string,
# MAGIC  dol_vehicle_id string ,
# MAGIC  geocoded_column string ,
# MAGIC  electric_utility string ,
# MAGIC  census_tract string,
# MAGIC  computed_region_x4ys_rtnd string,
# MAGIC  computed_region_fny7_vc3j string,
# MAGIC  computed_region_8ddd_yn5v string,
# MAGIC  date_as_of_date integer
# MAGIC  )
# MAGIC USING DELTA
# MAGIC LOCATION 'Processed/ElectricVehicleData';
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * FROM delta.`/user/hive/warehouse/electricvehicledata`;
# MAGIC
