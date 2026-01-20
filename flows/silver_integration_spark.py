import os
import shutil
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
from prefect import flow, task
from utils.config import BUCKET_BRONZE, BUCKET_SILVER, get_minio_client, get_spark_session


@task(name="load_from_bronze")
def load_bronze_data(folder_name: str):
    """Télécharge les données CSV depuis MinIO et charge avec Spark."""
    client = get_minio_client()
    spark = get_spark_session()
    local_dir = f"./temp_bronze_{folder_name}"

    if os.path.exists(local_dir):
        shutil.rmtree(local_dir)
    os.makedirs(local_dir)

    # Télécharger tous les fichiers du dossier Bronze
    objects = list(client.list_objects(BUCKET_BRONZE, prefix=f"{folder_name}/", recursive=True))

    print(f"\n📦 Objets trouvés dans MinIO pour '{folder_name}/':")
    if not objects:
        raise FileNotFoundError(f"❌ Aucun objet trouvé dans {BUCKET_BRONZE}/{folder_name}/")

    for obj in objects:
        print(f"  - {obj.object_name} ({obj.size} bytes)")

        # Préserver la structure du chemin
        relative_path = obj.object_name.replace(f"{folder_name}/", "", 1)
        local_file = os.path.join(local_dir, relative_path)

        # Créer les sous-dossiers si nécessaire
        os.makedirs(os.path.dirname(local_file), exist_ok=True)

        # Télécharger le fichier
        client.fget_object(BUCKET_BRONZE, obj.object_name, local_file)

    # Vérifier les fichiers disponibles
    csv_files = list(Path(local_dir).rglob("*.csv"))

    print(f"📊 Fichiers CSV trouvés : {len(csv_files)}")
    for f in csv_files:
        print(f"  - {f}")

    if not csv_files:
        raise FileNotFoundError(f"❌ Aucun fichier CSV trouvé dans {local_dir}")

    # Lire les CSV avec Spark
    print(f"\n📖 Lecture CSV depuis {local_dir}")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(local_dir)

    print(f"✅ DataFrame chargé : {df.count()} lignes")
    df.printSchema()

    return df


@task(name="transform_clients")
def transform_clients(df):
    """Clean and standardize client data."""

    df = df.dropDuplicates(subset=['id_client'])

    if 'email' in df.columns:
        df = df.withColumn('email', F.trim(F.lower(F.col('email'))))

    df = df.withColumn('nom',
                       F.when(F.col('nom').isNull(), 'Inconnu')
                       .otherwise(F.col('nom')))

    df = df.withColumn('id_client', F.col('id_client').cast(IntegerType()))

    print(f"✅ Clients transformés : {df.count()} lignes")
    return df


@task(name="transform_purchases")
def transform_purchases(df):
    """Clean and standardize transactions (purchases)."""

    df = df.withColumn('date_achat', F.to_timestamp(F.col('date_achat')))

    df = df.withColumn('montant', F.col('montant').cast(DoubleType()))

    df = df.filter(F.col('montant').isNotNull())

    df = df.filter(F.col('montant') >= 0)

    df = df.withColumn('id_client', F.col('id_client').cast(IntegerType()))

    print(f"✅ purchases transformés : {df.count()} lignes")
    return df


@task(name="upload_to_silver")
def upload_to_silver_layer(df, folder_name: str):
    """Sauvegarde en Parquet local puis upload vers MinIO."""
    client = get_minio_client()
    local_output = f"./temp_silver_{folder_name}"

    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)

    # Spark écrit en Parquet
    df.write.mode("overwrite").parquet(local_output)

    # Upload vers MinIO
    uploaded_count = 0
    for root, _, files in os.walk(local_output):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_output)
            s3_path = f"{folder_name}/{relative_path}".replace("\\", "/")

            client.fput_object(BUCKET_SILVER, s3_path, local_path)
            uploaded_count += 1

    shutil.rmtree(local_output)
    print(f"✅ {folder_name}: {uploaded_count} fichiers uploadés vers Silver")


@flow(name="Silver Transformation Flow")
def silver_transformation_flow():
    print("\n" + "=" * 50)
    print("🚀 DÉMARRAGE SILVER TRANSFORMATION")
    print("=" * 50)

    # Charger depuis Bronze
    df_clients = load_bronze_data("clients")
    df_purchases = load_bronze_data("purchases")

    # Transformer
    clean_clients = transform_clients(df_clients)
    clean_purchases = transform_purchases(df_purchases)

    # Uploader vers Silver
    upload_to_silver_layer(clean_clients, "clients_clean")
    upload_to_silver_layer(clean_purchases, "purchases_clean")

    print("\n" + "=" * 50)
    print("✅ SILVER LAYER UPDATED SUCCESSFULLY!")
    print("=" * 50)


if __name__ == "__main__":
    silver_transformation_flow()