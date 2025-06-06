from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws

# Inizializza Spark
spark = SparkSession.builder.appName("Job1 - Spark Core").getOrCreate()

# Carica il CSV
df = spark.read.csv('/user/hadoop/us_used_cars_cleaned_job1.csv', header=True, inferSchema=True, sep=";")

# Conversione a RDD
rdd = df.rdd.map(lambda row: (
    row['make'],              # make_name
    row['model_name'],        # model_name
    float(row['price']),      # price
    int(row['year'])          # year
))

# Mappatura con chiave composta: (make, model)
mapped = rdd.map(lambda x: (
    (x[0], x[1]),             # chiave: (make, model)
    (x[2], x[2], x[2], 1, {x[3]})  # (min_price, max_price, sum_price, count, set anni)
))

# Funzione di riduzione
def reducer(a, b):
    return (
        min(a[0], b[0]),              # min_price
        max(a[1], b[1]),              # max_price
        a[2] + b[2],                  # sum_price
        a[3] + b[3],                  # count
        a[4].union(b[4])              # union anni
    )

# Aggregazione
aggregated = mapped.reduceByKey(reducer)

# Calcolo media e conversione finale
final_rdd = aggregated.map(lambda x: (
    x[0][0],                 # make_name
    x[0][1],                 # model_name
    x[1][3],                 # numero auto
    round(x[1][0], 2),       # prezzo minimo
    round(x[1][1], 2),       # prezzo massimo
    round(x[1][2] / x[1][3], 2),  # prezzo medio
    sorted(list(x[1][4]))    # anni (come lista ordinata)
))

# Conversione in DataFrame
result_df = final_rdd.toDF(["make_name", "model_name", "car_count", "min_price", "max_price", "avg_price", "years"])

# Concatena gli anni in una stringa per salvarli in CSV
result_df = result_df.withColumn("years", concat_ws(",", "years"))

# Ordinamento e salvataggio
result_df = result_df.orderBy("make_name", "model_name").coalesce(1)

# Visualizza i risultati
result_df.show(truncate=False)

# Salvataggio in CSV
result_df.write.csv('/user/hadoop/output_spark_core', header=True, mode='overwrite')