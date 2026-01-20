from io import BytesIO, StringIO
import pandas

from prefect import flow, task
from utils.config import BUCKET_SILVER, BUCKET_GOLD, get_minio_client

@task(name="load_from_silver", retries=2)
def load_from_silver(object_name: str) -> pandas.DataFrame:
    """_summary_

    Args:
        object_name (str): _description_

    Returns:
        pandas.DataFrame: _description_
    """

    client = get_minio_client()
    response = client.get_object(BUCKET_SILVER, object_name)
    data = response.read()
    response.close()
    response.release_conn()

    dataFrame = pandas.read_csv(BytesIO(data))
    return dataFrame

@task(name="create_dimension_tables", retries=2)
def create_dimension_tables(clients_dataFrame: pandas.DataFrame) -> dict:
    """_summary_

    Args:
        clients_dataFrame (pandas.DataFrame): _description_

    Returns:
        dict: _description_
    """

    dim_client = clients_dataFrame[["id_client", "nom", "email", "pays"]].copy()
    dim_client = dim_client.drop_duplicates(subset=["id_client"])

    return {"dim_client": dim_client}

@task(name="create_fact_table", retries=2)
def create_fact_table(purchases_dataFrame: pandas.DataFrame) -> pandas.DataFrame:
    """_summary_

    Args:
        purchases_dataFrame (pandas.DataFrame): _description_

    Returns:
        pandas.DataFrame: _description_
    """

    purchases_dataFrame["date_achat"] = pandas.to_datetime(purchases_dataFrame["date_achat"])

    purchases_dataFrame["day"] = purchases_dataFrame["date_achat"].dt.date
    purchases_dataFrame["week"] = purchases_dataFrame["date_achat"].dt.to_period("W").astype(str)
    purchases_dataFrame["month"] = purchases_dataFrame["date_achat"].dt.to_period("M").astype(str)

    fact_purchases = purchases_dataFrame[
        ["id_achat", "id_client", "date_achat", "montant", "produit", "day", "week", "month"]
    ].copy()

    return fact_purchases

@task(name="calculate_kpis", retries=2)
def calculate_kpis(fact_purchases: pandas.DataFrame, dim_client: pandas.DataFrame) -> dict:
    """_summary_

    Args:
        fact_purchases (pandas.DataFrame): _description_
        dim_client (pandas.DataFrame): _description_

    Returns:
        dict: _description_
    """

    kpi_volume_revenue = (
        fact_purchases.groupby(["month", "week", "day"])
        .agg(
            total_purchases=("id_achat", "count"),
            total_revenue=("montant", "sum"),
        )
        .reset_index()
    )

    merged_dataFrame = pandas.merge(fact_purchases, dim_client, on="id_client", how="left")
    kpi_revenue_country = (
        merged_dataFrame.groupby("pays")
        .agg(total_revenue=("montant", "sum"))
        .reset_index()
    )

    monthly_revenue = (
        fact_purchases.groupby("month")
        .agg(total_revenue=("montant", "sum"))
        .reset_index()
    )
    monthly_revenue["growth_rate"] = (
        monthly_revenue["total_revenue"].pct_change() * 100
    )

    kpi_stats = {
        "avg_purchase_value": fact_purchases["montant"].mean(),
        "median_purchase_value": fact_purchases["montant"].median(),
        "std_purchase_value": fact_purchases["montant"].std(),
    }

    return {
        "volume_revenue": kpi_volume_revenue,
        "revenue_country": kpi_revenue_country,
        "monthly_growth": monthly_revenue,
        "stats": kpi_stats,
    }

@task(name="save_to_gold", retries=2)
def save_to_gold(dataFrame: pandas.DataFrame, object_name: str) -> str:
    """_summary_

    Args:
        dataFrame (pandas.DataFrame): _description_
        object_name (str): _description_

    Returns:
        str: _description_
    """
    
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_GOLD):
        client.make_bucket(BUCKET_GOLD)

    csv_buffer = StringIO()
    dataFrame.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    client.put_object(
        BUCKET_GOLD,
        object_name,
        BytesIO(csv_bytes),
        length=len(csv_bytes),
    )
    print(f"Saved {object_name} to {BUCKET_GOLD}")
    return object_name

@flow(name="Gold Aggregation Flow")
def gold_aggregation_flow() -> dict:
    """_summary_

    Returns:
        dict: _description_
    """
    
    clients_dataFrame = load_from_silver("clients_clean.csv")
    purchases_dataFrame = load_from_silver("purchases_clean.csv")

    dim_tables = create_dimension_tables(clients_dataFrame)
    fact_purchases = create_fact_table(purchases_dataFrame)

    kpis = calculate_kpis(fact_purchases, dim_tables["dim_client"])

    save_to_gold(dim_tables["dim_client"], "dim_client.csv")
    save_to_gold(fact_purchases, "fact_purchases.csv")
    save_to_gold(kpis["volume_revenue"], "kpi_volume_revenue.csv")
    save_to_gold(kpis["revenue_country"], "kpi_revenue_country.csv")
    save_to_gold(kpis["monthly_growth"], "kpi_monthly_growth.csv")

    return {
        "dim_client": "dim_client.csv",
        "fact_purchases": "fact_purchases.csv",
        "kpi_volume_revenue": "kpi_volume_revenue.csv",
        "kpi_revenue_country": "kpi_revenue_country.csv",
        "kpi_monthly_growth": "kpi_monthly_growth.csv",
        "stats": kpis["stats"],
    }

if __name__ == "__main__":
    result = gold_aggregation_flow()
    print(f"Gold aggregation complete: {result}")
