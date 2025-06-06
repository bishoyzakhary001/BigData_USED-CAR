from pyspark.sql import SparkSession

# Inizializza Spark
spark = SparkSession.builder.appName("Auto Motori Simili - Spark Core").getOrCreate()

# Caricamento CSV
df = spark.read.csv('/user/hadoop/us_used_cars_Fixex_job3.csv', header=True, inferSchema=True)

# Conversione a RDD per elaborazione manuale
rdd = df.rdd.map(lambda row: (
    row['model_name'],
    float(row['horsepower']),
    float(row['engine_displacement']),
    float(row['price'])
))

# Funzione per calcolare il group_id (hp_bin e disp_bin)
def group_mapper(row):
    model, hp, disp, price = row
    if disp < 30:
        disp *= 1000  # correzione automatica da litri a cc
    hp_bin = int(hp / 10)
    disp_bin = int(disp / 100)
    key = f"{hp_bin}-{disp_bin}"
    return (key, (model, price, hp, disp))

# Mappa e raggruppa per group_id
grouped_rdd = rdd.map(group_mapper).groupByKey()

# Funzione di aggregazione: media prezzi + modello con max horsepower
def compute_aggregates(values):
    values = list(values)
    avg_price = round(sum(v[1] for v in values) / len(values), 2)
    top_model = max(values, key=lambda x: x[2])[0]
    return (avg_price, top_model)

# Applica aggregazione
aggregated_rdd = grouped_rdd.mapValues(compute_aggregates)

# Ordina per group_id
ordered_rdd = aggregated_rdd.sortBy(lambda x: x[0])

# Ritorna come DataFrame ordinato
result_df = ordered_rdd.map(lambda x: (
    x[0], x[1][0], x[1][1]
)).toDF(["group_id", "avg_price", "top_model"])

# Riduce a una partizione e mostra
result_df = result_df.coalesce(1)
result_df.show()

# Salva su HDFS o locale
result_df.write.csv('/user/hadoop/job3_spark_core_output', header=True, mode='overwrite')
