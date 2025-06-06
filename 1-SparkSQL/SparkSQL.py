from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, collect_set, count, min, max, avg,concat_ws

# Inizializza la sessione Spark
spark = SparkSession.builder.appName("Auto Statistiche per Marca e Modello - Spark SQL").getOrCreate()

# Caricamento CSV con separatore corretto
df = spark.read.option("header", True)\
    .option("inferSchema", True)\
    .option("sep", ";")\
    .csv("/user/hadoop/us_used_cars_cleaned_job1.csv")

# Crea una vista SQL temporanea
df.createOrReplaceTempView("cars")

# Esegui la query SQL
# Query con array -> stringa
result_df = spark.sql("""
    SELECT 
        make AS make_name,
        model_name,
        COUNT(*) AS car_count,
        ROUND(MIN(price), 2) AS min_price,
        ROUND(MAX(price), 2) AS max_price,
        ROUND(AVG(price), 2) AS avg_price,
        COLLECT_SET(year) AS year_list
    FROM cars
    GROUP BY make, model_name
""")

# Converti l’array di anni in stringa con virgole
result_df = result_df.withColumn("years", concat_ws(",", result_df["year_list"])) \
                     .drop("year_list") \
                     .orderBy("make_name", "model_name") \
                     .coalesce(1)

# Mostra anteprima
result_df.show(truncate=False)

# Salva su CSV
result_df.write.csv("/user/hadoop/output_spark_sql", header=True, mode="overwrite")