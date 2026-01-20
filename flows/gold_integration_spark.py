import os
import shutil
import time
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from prefect import flow, task
from config import BUCKET_SILVER, BUCKET_GOLD, get_minio_client, get_spark_session


@task(name="load_silver_data_spark")
def load_silver_data(folder_name: str):
    """Télécharge les données Parquet du Silver en local et les charge avec Spark."""
    client = get_minio_client()
    spark = get_spark_session()
    local_dir = f"./temp_silver_{folder_name}"

    if os.path.exists(local_dir):
        shutil.rmtree(local_dir)

    # Téléchargement depuis MinIO (Bucket Silver)
    objects = list(client.list_objects(BUCKET_SILVER, prefix=f"{folder_name}/", recursive=True))
    for obj in objects:
        local_file = os.path.join(local_dir, obj.object_name.replace(f"{folder_name}/", "", 1))
        os.makedirs(os.path.dirname(local_file), exist_ok=True)
        client.fget_object(BUCKET_SILVER, obj.object_name, local_file)

    # Lecture Spark
    return spark.read.parquet(local_dir)


@task(name="create_gold_metrics_spark")
def compute_gold_metrics(df_clients, df_achats):
    """Calcule les KPIs métier avec PySpark."""
    start_time = time.time()

    # 1. Jointure
    df_merged = df_achats.join(df_clients, "id_client", "inner")

    # 2. Création des colonnes temporelles
    # En Spark, on utilise date_format pour le mois (YYYY-MM)
    df_merged = df_merged.withColumn("mois", F.date_format(F.col("date_achat"), "yyyy-MM"))

    # 3. Agrégation par Pays et Mois
    ca_pays_mois = df_merged.groupby("pays", "mois").agg(
        F.sum("montant").alias("total_ca"),
        F.count("id_achat").alias("nombre_achats"),
        F.mean("montant").alias("panier_moyen")
    )

    # 4. Calcul de la croissance (Pct Change) avec une Window Function
    # C'est l'équivalent Spark du pct_change() de Pandas
    window_spec = Window.partitionBy("pays").orderBy("mois")

    ca_pays_mois = ca_pays_mois.withColumn(
        "prev_ca", F.lag("total_ca").over(window_spec)
    )

    ca_pays_mois = ca_pays_mois.withColumn(
        "croissance_ca_pct",
        F.round(((F.col("total_ca") - F.col("prev_ca")) / F.col("prev_ca")) * 100, 2)
    ).drop("prev_ca")

    duration = time.time() - start_time
    print(f"⏱️ Temps de transformation Gold (Spark) : {duration:.4f}s")

    return ca_pays_mois


@task(name="upload_to_gold_spark")
def upload_to_gold_layer(df, folder_name: str):
    """Sauvegarde les KPIs en Parquet et upload vers MinIO."""
    client = get_minio_client()
    local_output = f"./temp_gold_{folder_name}"

    # Spark écrit en Parquet localement
    df.write.mode("overwrite").parquet(local_output)

    # Upload vers MinIO
    if not client.bucket_exists(BUCKET_GOLD):
        client.make_bucket(BUCKET_GOLD)

    for root, _, files in os.walk(local_output):
        for file in files:
            local_path = os.path.join(root, file)
            s3_path = f"{folder_name}/{os.path.relpath(local_path, local_output)}".replace("\\", "/")
            client.fput_object(BUCKET_GOLD, s3_path, local_path)

    shutil.rmtree(local_output)


@flow(name="Gold Ingestion Flow PySpark")
def gold_transformation_flow():
    # On charge les dossiers Parquet créés par le flow Silver
    df_clients = load_silver_data("clients_clean")
    df_achats = load_silver_data("achats_clean")

    # Transformation
    df_kpi_pays = compute_gold_metrics(df_clients, df_achats)

    # Export
    upload_to_gold_layer(df_kpi_pays, "kpi_ca_pays_mensuel")


if __name__ == "__main__":
    gold_transformation_flow()