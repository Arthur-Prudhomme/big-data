import streamlit
import pandas
import plotly.express as px
from io import BytesIO
from utils.config import BUCKET_GOLD, get_minio_client

def load_data_from_minio(bucket_name: str, object_name: str) -> pandas.DataFrame:
    client = get_minio_client()
    response = client.get_object(bucket_name, object_name)
    data = response.read()
    response.close()
    response.release_conn()
    return pandas.read_csv(BytesIO(data))

@streamlit.cache_data
def load_all_data():
    data = {
        "fact_purchases": load_data_from_minio(BUCKET_GOLD, "fact_purchases.csv"),
        "kpi_volume_revenue": load_data_from_minio(BUCKET_GOLD, "kpi_volume_revenue.csv"),
        "kpi_revenue_country": load_data_from_minio(BUCKET_GOLD, "kpi_revenue_country.csv"),
        "kpi_monthly_growth": load_data_from_minio(BUCKET_GOLD, "kpi_monthly_growth.csv"),
    }
    return data

def main():
    streamlit.title("Youpi Dashboard")
    streamlit.write("Los statisticos dé la couche de gold, davai")

    data = load_all_data()

    streamlit.sidebar.title("Directionnage")
    page = streamlit.sidebar.radio(
        "Se mouvoir vers",
        ["Observance des Achètements", "Volumétrie and Revenussage", "Revenancement par Territoire-National-Indépendant", "Croissant au beurre par Mois", "Regardage des Produits"]
    )

    if page == "Observance des Achètements":
        streamlit.header("Observance des Achètements")
        streamlit.dataframe(data["fact_purchases"].head(100))
        streamlit.write(f"Total Achètements: {len(data['fact_purchases'])}")
        streamlit.write(f"Total Revenussage: {data['fact_purchases']['montant'].sum():.2f} €")

        fig = px.histogram(data["fact_purchases"], x="montant", nbins=20, title="Reparticention des Achètements")
        streamlit.plotly_chart(fig)

    elif page == "Volumétrie and Revenussage":
        streamlit.header("Volumétrie and Revenussage")
        streamlit.dataframe(data["kpi_volume_revenue"])

        fig = px.line(
            data["kpi_volume_revenue"],
            x="month",
            y="total_revenue",
            title="Revenussage"
        )
        streamlit.plotly_chart(fig)

        fig = px.bar(
            data["kpi_volume_revenue"],
            x="month",
            y="total_purchases",
            title="Quantité d'Achètements"
        )
        streamlit.plotly_chart(fig)

    elif page == "Revenancement par Territoire-National-Indépendant":
        streamlit.header("Revenancement par Territoire-National-Indépendant")
        streamlit.dataframe(data["kpi_revenue_country"])

        fig = px.bar(
            data["kpi_revenue_country"],
            x="pays",
            y="total_revenue",
            title="Revenancement par Territoire-National-Indépendant"
        )
        streamlit.plotly_chart(fig)

        fig = px.pie(
            data["kpi_revenue_country"],
            values="total_revenue",
            names="pays",
            title="Revenancement camembert par Territoire-National-Indépendant"
        )
        streamlit.plotly_chart(fig)

    elif page == "Croissant au beurre par Mois":
        streamlit.header("Croissant au beurre par Mois")
        streamlit.dataframe(data["kpi_monthly_growth"])

        fig = px.line(
            data["kpi_monthly_growth"],
            x="month",
            y="growth_rate",
            title="Croissant au beurre par Mois"
        )
        streamlit.plotly_chart(fig)

    elif page == "Regardage des Produits":
        streamlit.header("Regardage des Produits")
        product_revenue = (
            data["fact_purchases"]
            .groupby("produit")
            .agg(total_revenue=("montant", "sum"), total_purchases=("id_achat", "count"))
            .reset_index()
        )
        streamlit.dataframe(product_revenue)

        fig = px.bar(
            product_revenue,
            x="produit",
            y="total_revenue",
            title="Revenancement par Produits"
        )
        streamlit.plotly_chart(fig)

        fig = px.pie(
            product_revenue,
            values="total_purchases",
            names="produit",
            title="Achètements camembert par Produits"
        )
        streamlit.plotly_chart(fig)

if __name__ == "__main__":
    main()
