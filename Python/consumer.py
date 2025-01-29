from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Créer une session Spark avec le support pour Kafka et MinIO
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("fs.s3a.endpoint", "http://127.0.0.1:9000") \
    .config("fs.s3a.access.key", "minio") \
    .config("fs.s3a.secret.key", "minio123") \
    .config("fs.s3a.path.style.access", "true") \
    .getOrCreate()


# Définir le schéma des données JSON envoyées par le producteur
schema = StructType([
    StructField("id_transaction", StringType(), True),
    StructField("type_transaction", StringType(), True),
    StructField("montant", FloatType(), True),
    StructField("devise", StringType(), True),
    StructField("date", StringType(), True),
    StructField("lieu", StringType(), True),
    StructField("moyen_paiement", StringType(), True),
    StructField("details", StructType([
        StructField("produit", StringType(), True),
        StructField("quantite", FloatType(), True),
        StructField("prix_unitaire", FloatType(), True)
    ]), True),
    StructField("utilisateur", StructType([
        StructField("id_utilisateur", StringType(), True),
        StructField("nom", StringType(), True),
        StructField("adresse", StringType(), True),
        StructField("email", StringType(), True)
    ]), True)
])

# Lire les messages Kafka
raw_kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "transactions") \
    .load()

# Extraire la colonne 'value' (messages JSON sous forme de chaîne)
kafka_values_df = raw_kafka_df.selectExpr("CAST(value AS STRING) as json_data")

# Convertir les messages JSON en colonnes structurées (DataFrame)
transactions_df = kafka_values_df.select(
    from_json(col("json_data"), schema).alias("data")
).select("data.*")

# Appliquer les transformations demandées
transformed_df = transactions_df.withColumn("montant_eur", col("montant") * lit(0.85)) \
    .withColumn("timezone", lit("UTC")) \
    .withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ss")) \
    .filter(col("moyen_paiement") != "erreur") \
    .filter(col("utilisateur.adresse").isNotNull())

# Écrire les données transformées dans MinIO au format Parquet
query = transformed_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3a://bigdata/transactions") \
    .option("checkpointLocation", "s3a://bigdata/checkpoints/transactions") \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Arrêt demandé par l'utilisateur.")
finally:
    query.stop()
    spark.stop()
    print("Streaming et Spark arrêtés correctement.")
