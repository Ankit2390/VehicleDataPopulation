# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dim_date (
# MAGIC     date_id INT,
# MAGIC     full_date DATE,
# MAGIC     year INT,
# MAGIC     month INT,
# MAGIC     day INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/Provisioned/dim_date';

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, monotonically_increasing_id

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PopulateDateDimension") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define the date range
start_date = "2010-01-01"
end_date = "2030-12-31"

# Generate a DataFrame with a sequence of dates
df = spark.sql(f"""
    SELECT sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) as dates
""").selectExpr("explode(dates) as full_date")

# Extract date attributes
df = df.withColumn("date_id", expr("year(full_date) * 10000 + month(full_date) * 100 + day(full_date)")) \
       .withColumn("year", expr("year(full_date)")) \
       .withColumn("month", expr("month(full_date)")) \
       .withColumn("day", expr("day(full_date)"))

# Define Delta table path
delta_table_path = "/Provisioned/dim_date"

# Save as Delta Table
df.write.format("delta") \
    .mode("overwrite") \
    .save(delta_table_path)

print(f"Date Dimension populated successfully in {delta_table_path}")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_date
