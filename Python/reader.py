from pyspark.sql import SparkSession

# Créer une session Spark avec le support pour MinIO
spark = SparkSession.builder \
    .appName("ReadMinIOParquet") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Lire les fichiers Parquet depuis MinIO
df = spark.read.parquet("s3a://bigdata/transactions")

# Afficher des lignes pour vérifier
df.show(50, truncate=False)

# Arrêter Spark
spark.stop()
