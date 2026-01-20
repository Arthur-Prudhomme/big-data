import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

def generate_purchases(clients_ids: list[int], avg_purchases_per_client: int, output_path: str) -> None:
    """
    Generate fake client data

    Args:
        clients_ids: List of client IDs
        id_achat, id_client, data_achat, montant, produit
        avg_purchases_per_client: Average number of purchases per client
        output_path (str): Path to save the client csv file

    """

    products = ["Laptop", "Phone", "Tablet", "Headphones", "Monitor",
                "Keyboard", "Mouse", "Webcam", "Speaker", "Charger"]

    purchases = []

    for client_id in clients_ids:
        n_purchases = max(1, int(random.normalvariate(avg_purchases_per_client, avg_purchases_per_client / 2)))

        for _ in range(n_purchases):
            purchase_date = fake.date_between(start_date="-1y", end_date="today")
            amount = round(random.uniform(10, 1000), 2)
            product = random.choice(products)

            purchases.append({
                "id_achat": len(purchases) + 1,
                "id_client": client_id,
                "date_achat": purchase_date.strftime("%Y-%m-%d"),
                "montant": amount,
                "produit": product
            })

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id_achat", "id_client", "date_achat", "montant", "produit"])
        writer.writeheader()
        writer.writerows(purchases)

    print(f"Generated {len(purchases)} purchases in file {output_path}")

if __name__ == "__main__":
    output_dir = Path(__file__).parent.parent / "data" / "sources"

    clients_ids = list(range(1, 1501))
    generate_purchases(
        clients_ids=clients_ids,
        avg_purchases_per_client=5,
        output_path=str(output_dir / "purchases.csv")
    )
