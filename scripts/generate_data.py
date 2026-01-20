import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

fake= Faker()
Faker.seed(42)
random.seed(42)

def generate_clients(n_clients: int, output_path: str) -> list[int]:
    """
    Generate fake client data

    Args:
        n_clients (int): Numer of client to generate
        output_path (str): Path to save the client csv file

    Returns:
        list[int]: List of client IDs
    """

    countries = ["France", "Germany", "Spain", "Italy", "Belgium", 
                 "Netherland", "Switzerland", "UK", "Canada"]
    
    clients = []
    client_ids = []

    for i in range(1, n_clients + 1):
        date_inscription = fake.date_between(start_date = "-3y", end_date= "-1m")
        clients.append(
            {
                "id_client": i,
                "nom": fake.name(),
                "email": fake.email(),
                "date_inscription": date_inscription.strftime("%Y-%m-%d"),
                "pays": random.choice(countries)
            }
        )
        client_ids.append(i)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", newline="", encoding= "utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id_client", "nom", "email", "date_inscription", "pays"])
        writer.writeheader()
        writer.writerows(clients)

    print(f"Generated Clients: {n_clients} in file {output_path} ")
    return client_ids

if __name__ == "__main__":
    output_dir = Path(__file__).parent.parent / "data" / "sources"

    clients_ids = generate_clients(
        n_clients= 10000,
        output_path=str(output_dir / "clients.csv")
    )