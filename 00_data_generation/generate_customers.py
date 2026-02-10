"""
Generate synthetic customer data as CSV files.
Includes realistic PII fields for practicing data governance and masking.
"""

import csv
import os
import random
from faker import Faker

from config import (
    NUM_CUSTOMERS,
    CUSTOMERS_DIR,
    REGIONS,
    RANDOM_SEED,
)

fake = Faker()
Faker.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)


def get_region(state_abbr: str) -> str:
    """Map a state abbreviation to its region."""
    for region, states in REGIONS.items():
        if state_abbr in states:
            return region
    return "Unknown"


def generate_customers():
    """Generate customer CSV file with PII and demographic data."""
    os.makedirs(CUSTOMERS_DIR, exist_ok=True)
    filepath = os.path.join(CUSTOMERS_DIR, "customers.csv")

    all_states = [state for states in REGIONS.values() for state in states]

    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "customer_id",
                "first_name",
                "last_name",
                "email",
                "phone",
                "address",
                "city",
                "state",
                "zip_code",
                "region",
                "date_of_birth",
                "registration_date",
                "loyalty_tier",
            ],
        )
        writer.writeheader()

        for i in range(1, NUM_CUSTOMERS + 1):
            state = random.choice(all_states)
            writer.writerow(
                {
                    "customer_id": f"CUST-{i:06d}",
                    "first_name": fake.first_name(),
                    "last_name": fake.last_name(),
                    "email": fake.email(),
                    "phone": fake.phone_number(),
                    "address": fake.street_address(),
                    "city": fake.city(),
                    "state": state,
                    "zip_code": fake.zipcode(),
                    "region": get_region(state),
                    "date_of_birth": fake.date_of_birth(
                        minimum_age=18, maximum_age=80
                    ).isoformat(),
                    "registration_date": fake.date_between(
                        start_date="-3y", end_date="today"
                    ).isoformat(),
                    "loyalty_tier": random.choice(
                        ["Bronze", "Silver", "Gold", "Platinum"]
                    ),
                }
            )

    print(f"Generated {NUM_CUSTOMERS} customers -> {filepath}")


if __name__ == "__main__":
    generate_customers()
