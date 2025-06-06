from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max as spark_max, row_number
from pyspark.sql.window import Window

# Inizializza una sessione Spark
spark = SparkSession.builder.appName("Job3 Spark SQL - Motori Simili").getOrCreate()

# Carica i dati
df = spark.read.csv('/user/hadoop/us_used_cars_Fixex_job3.csv', header=True, inferSchema=True)

# Crea una vista temporanea
df.createOrReplaceTempView("cars")

# Aggiungi colonne di raggruppamento (binning)
binned_df = spark.sql("""
    SELECT *,
           CAST(horsepower / 10 AS INT) AS hp_bin,
           CAST(CASE WHEN engine_displacement < 30 THEN engine_displacement * 1000 ELSE engine_displacement END / 100 AS INT) AS disp_bin
    FROM cars
""")

# Crea una nuova vista con il campo group_id
binned_df.createOrReplaceTempView("binned_cars")

grouped_df = spark.sql("""
    SELECT *,
           CONCAT(hp_bin, '-', disp_bin) AS group_id
    FROM binned_cars
""")

# Crea vista temporanea per le query successive
grouped_df.createOrReplaceTempView("grouped_cars")

# Calcola il prezzo medio per ciascun gruppo
avg_price_df = spark.sql("""
    SELECT group_id, ROUND(AVG(price), 2) AS avg_price
    FROM grouped_cars
    GROUP BY group_id
""")

# Finestra per trovare il modello con maggiore horsepower per ogni gruppo
window_spec = Window.partitionBy("group_id").orderBy(col("horsepower").desc())

# Aggiunta del modello top per ogni gruppo
top_model_df = grouped_df.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .select("group_id", col("model_name").alias("top_model"))

# Unione finale
result_df = avg_price_df.join(top_model_df, on="group_id")

# Riduci a una sola partizione e mostra il risultato
result_df = result_df.coalesce(1)
result_df.show()

# Salva il risultato in HDFS
result_df.write.csv('/user/hadoopt/job3_sparksql_output', header=True, mode='overwrite')