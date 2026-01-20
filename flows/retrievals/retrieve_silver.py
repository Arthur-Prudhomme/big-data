from io import BytesIO
from pathlib import Path
import pandas

from prefect import flow, task

from ..utils.config import BUCKET_SILVER, get_minio_client

def download_and_inspect(bucket_name: str, object_name: str) -> pandas.DataFrame:
    """_summary_

    Args:
        bucket_name (str): _description_
        object_name (str): _description_

    Returns:
        pandas.DataFrame: _description_
    """
    client = get_minio_client()
    response = client.get_object(bucket_name, object_name)
    data = response.read()
    response.close()
    response.release_conn()

    df = pandas.read_csv(BytesIO(data))
    return df

clients_df = download_and_inspect(BUCKET_SILVER, "clients_clean.csv")
print("First 5 rows of clients_clean.csv:")
print(clients_df.head())

purchases_df = download_and_inspect(BUCKET_SILVER, "purchases_clean.csv")
print("\nFirst 5 rows of purchases_clean.csv:")
print(purchases_df.head())