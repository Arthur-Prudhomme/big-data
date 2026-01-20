from io import BytesIO
from pathlib import Path
import pandas

from prefect import flow, task

from ..utils.config import BUCKET_GOLD, get_minio_client

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

    dataFrame = pandas.read_csv(BytesIO(data))
    return dataFrame

fact_purchases_dataFrame = download_and_inspect(BUCKET_GOLD, "fact_purchases.csv")
print("First 5 rows of fact_purchases.csv:")
print(fact_purchases_dataFrame.head())

kpi_volume_revenue_dataFrame = download_and_inspect(BUCKET_GOLD, "kpi_volume_revenue.csv")
print("\nFirst 5 rows of kpi_volume_revenue.csv:")
print(kpi_volume_revenue_dataFrame.head())

kpi_revenue_country_dataFrame = download_and_inspect(BUCKET_GOLD, "kpi_revenue_country.csv")
print("\nFirst 5 rows of kpi_revenue_country.csv:")
print(kpi_revenue_country_dataFrame.head())

kpi_monthly_growth_dataFrame = download_and_inspect(BUCKET_GOLD, "kpi_monthly_growth.csv")
print("\nFirst 5 rows of kpi_monthly_growth.csv:")
print(kpi_monthly_growth_dataFrame.head())
