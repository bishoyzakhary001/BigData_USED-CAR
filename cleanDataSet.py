from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, split, explode, trim, countDistinct
from pyspark.sql.types import DoubleType, IntegerType


spark = SparkSession.builder \
    .appName("UsedCarsCleaning") \
    .getOrCreate()

df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv("used_cars_data.csv")


df = df_raw.select(
    "make_name", "model_name", "price", "year", "city", 
    "daysonmarket", "description", "horsepower", "engine_displacement"
)

df_cleaned = df.dropna(subset=[
    "make_name", "model_name", "price", "year", "city", 
    "daysonmarket", "description", "horsepower", "engine_displacement"
])


df_cleaned = df_cleaned.withColumn("make_name", trim(col("make_name")))
df_cleaned = df_cleaned.withColumn("model_name", trim(col("model_name")))
df_cleaned = df_cleaned.withColumn("city", trim(col("city")))

# 10. Salva il dataset 
df_cleaned.write.mode("overwrite").parquet("used_cars_cleaned.parquet")