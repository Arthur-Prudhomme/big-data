from io import BytesIO, StringIO
from pathlib import Path
import pandas

from prefect import flow, task
from utils.config import BUCKET_BRONZE, BUCKET_SILVER, get_minio_client

@task(name="load_from_bronze")
def load_from_bronze(object_name: str) -> pandas.DataFrame:
    """_summary_

    Args:
        object_name (str): _description_

    Returns:
        pandas.DataFrame: _description_
    """
    
    client = get_minio_client()
    response = client.get_object(BUCKET_BRONZE, object_name)
    data = response.read()
    response.close()
    response.release_conn()

    dataFrame = pandas.read_csv(BytesIO(data))
    return dataFrame

@task(name="clean_and_transform_clients")
def clean_and_transform_clients(dataFrame: pandas.DataFrame) -> pandas.DataFrame:
    """_summary_

    Args:
        dataFrame (pandas.DataFrame): _description_

    Returns:
        pandas.DataFrame: _description_
    """

    dataFrame = dataFrame.dropna()

    dataFrame["date_inscription"] = pandas.to_datetime(dataFrame["date_inscription"]).dt.strftime("%Y-%m-%d")

    dataFrame["id_client"] = dataFrame["id_client"].astype(int)
    dataFrame["nom"] = dataFrame["nom"].astype(str)
    dataFrame["email"] = dataFrame["email"].astype(str)
    dataFrame["pays"] = dataFrame["pays"].astype(str)

    dataFrame = dataFrame.drop_duplicates(subset=["id_client", "email"])

    return dataFrame

@task(name="clean_and_transform_purchases")
def clean_and_transform_purchases(dataFrame: pandas.DataFrame) -> pandas.DataFrame:
    """_summary_

    Args:
        dataFrame (pandas.DataFrame): _description_

    Returns:
        pandas.DataFrame: _description_
    """

    dataFrame = dataFrame.dropna()

    dataFrame["date_achat"] = pandas.to_datetime(dataFrame["date_achat"]).dt.strftime("%Y-%m-%d")

    dataFrame["id_achat"] = dataFrame["id_achat"].astype(int)
    dataFrame["id_client"] = dataFrame["id_client"].astype(int)
    dataFrame["montant"] = dataFrame["montant"].astype(float)
    dataFrame["produit"] = dataFrame["produit"].astype(str)

    dataFrame = dataFrame.drop_duplicates(subset=["id_achat", "id_client", "date_achat", "montant"])

    return dataFrame

@task(name="save_to_silver")
def save_to_silver(dataFrame: pandas.DataFrame, object_name: str) -> str:
    """_summary_

    Args:
        dataFrame (pandas.DataFrame): _description_
        object_name (str): _description_

    Returns:
        str: _description_
    """

    client = get_minio_client()

    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)

    csv_buffer = StringIO()
    dataFrame.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    client.put_object(
        BUCKET_SILVER,
        object_name,
        BytesIO(csv_bytes),
        length=len(csv_bytes)
    )
    print(f"Saved {object_name} to {BUCKET_SILVER}")
    return object_name

@flow(name="Silver Transformation Flow")
def silver_transformation_flow() -> dict:
    """_summary_

    Returns:
        dict: _description_
    """

    clients_dataFrame = load_from_bronze("clients.csv")
    purchases_dataFrame = load_from_bronze("purchases.csv")

    clients_clean = clean_and_transform_clients(clients_dataFrame)
    purchases_clean = clean_and_transform_purchases(purchases_dataFrame)

    silver_clients = save_to_silver(clients_clean, "clients_clean.csv")
    silver_purchases = save_to_silver(purchases_clean, "purchases_clean.csv")

    return {
        "clients": silver_clients,
        "purchases": silver_purchases
    }

if __name__ == "__main__":
    result = silver_transformation_flow()
    print(f"Silver transformation complete: {result}")
