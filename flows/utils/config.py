import os
from  pathlib import Path

from dotenv import load_dotenv
from minio import Minio

from pyspark.sql import SparkSession

load_dotenv()

# MinIO configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "False").lower() == "true"

# Database configuration
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "./data/database/analytics.db")

# Prefect configuration
PREFECT_API_URL = os.getenv("PREFECT_API_URL", "http://localhost:4200/api")

# Buckets
BUCKET_SOURCES = "sources"
BUCKET_BRONZE = "bronze"
BUCKET_SILVER = "silver"
BUCKET_GOLD = "gold"

def get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )
    
def configure_prefect() -> None:
    os.environ["PREFECT_API_URL"] = PREFECT_API_URL

def get_spark_session(app_name: str = "ELT_Pipeline") -> SparkSession:
    # 1. On s'assure qu'Hadoop n'essaie pas de charger des fichiers XML externes
    # Remplace par ton chemin réel vers le dossier contenant le dossier 'bin'
    os.environ['HADOOP_HOME'] = "C:\\Spark"
    # Ajoute le dossier bin au PATH pour que Java trouve les binaires natifs
    os.environ['PATH'] = os.environ['HADOOP_HOME'] + "\\bin;" + os.environ['PATH']

    builder = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.listing.cache.expiration", "3600") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10") \
        .config("spark.hadoop.fs.s3a.retry.limit", "10") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    spark = builder.getOrCreate()

    # 3. Injection forcée dans le contexte Java actif
    h_conf = spark._jsc.hadoopConfiguration()

    h_conf.set("fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
    h_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    h_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    h_conf.set("fs.s3a.path.style.access", "true")
    h_conf.set("fs.s3a.connection.ssl.enabled", "false")

    # Suppression spécifique du bug 24h/60s au niveau du driver
    h_conf.set("fs.s3a.listing.cache.expiration", "3600")
    h_conf.set("fs.s3a.connection.timeout", "60000")

    return spark

if __name__ == "__main__":
    client = get_minio_client()
    print(client.list_buckets())

    spark = get_spark_session()
    print("Spark est prêt !")

    configure_prefect()
    print(os.getenv("PREFECT_API_URL"))