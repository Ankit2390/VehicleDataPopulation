# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, explode
import json

# Load JSON file
file_path = "/FileStore/tables/Test/ElectricVehiclePopulationData.json"
df = spark.read.json(file_path, multiLine=True)
json_str= df.toJSON().first()

json_data=json.loads(json_str)

# Extract the relevant data
df_flattened = df.select(explode(col("data")).alias("record"))

# Extract column names dynamically from meta.view.columns
columns = [col["fieldName"] for col in json_data["meta"]["view"]["columns"]]

# Convert exploded data into a structured DataFrame
df_structured = df_flattened.select([col("record")[i].alias(columns[i]) for i in range(len(columns))])

# Show sample result
#df_structured.show(truncate=False)
#df_structured.display()

#add column for partition
df_structured = df_structured.withColumn("date_as_of_date", date_format(current_date(), "yyyyMMdd"))
df_structured.display()
#load in parquet format
output_path = "dbfs:/FileStore/tables/Test/Flatten_Vehicle_data.parquet"
df_structured.write.mode("overwrite").partitionBy("date_as_of_date").parquet(output_path)
