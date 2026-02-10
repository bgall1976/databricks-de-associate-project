"""
Generate synthetic product catalog as a CSV file.
"""

import csv
import os
import random
from faker import Faker

from config import (
    NUM_PRODUCTS,
    PRODUCTS_DIR,
    PRODUCT_CATEGORIES,
    RANDOM_SEED,
)

fake = Faker()
Faker.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)

# Product name templates by category
PRODUCT_TEMPLATES = {
    "Electronics": ["Wireless {}", "Smart {}", "Bluetooth {}", "USB-C {}"],
    "Clothing": ["{} Fit Shirt", "{} Jacket", "Classic {}", "Premium {}"],
    "Home & Kitchen": ["{} Blender", "{} Cookware Set", "Stainless {}"],
    "Books": ["The {} Guide", "Advanced {}", "Introduction to {}"],
    "Sports & Outdoors": ["{} Running Shoes", "{} Yoga Mat", "Pro {}"],
    "Health & Beauty": ["{} Moisturizer", "Organic {}", "Natural {}"],
    "Toys & Games": ["{} Board Game", "Interactive {}", "Classic {}"],
    "Automotive": ["{} Car Charger", "{} Dash Cam", "Premium {}"],
    "Garden & Outdoor": ["{} Garden Hose", "{} Planter", "Solar {}"],
    "Office Supplies": ["{} Desk Organizer", "{} Notebook Set", "Ergonomic {}"],
}

ADJECTIVES = [
    "Ultra", "Pro", "Elite", "Essential", "Premium", "Compact",
    "Deluxe", "Advanced", "Classic", "Modern", "Eco", "Turbo",
]


def generate_products():
    """Generate product catalog CSV file."""
    os.makedirs(PRODUCTS_DIR, exist_ok=True)
    filepath = os.path.join(PRODUCTS_DIR, "products.csv")

    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "product_id",
                "product_name",
                "category",
                "subcategory",
                "unit_price",
                "cost_price",
                "weight_kg",
                "supplier",
                "is_active",
            ],
        )
        writer.writeheader()

        for i in range(1, NUM_PRODUCTS + 1):
            category = random.choice(PRODUCT_CATEGORIES)
            templates = PRODUCT_TEMPLATES.get(category, ["{} Product"])
            template = random.choice(templates)
            adj = random.choice(ADJECTIVES)
            product_name = template.format(adj)

            unit_price = round(random.uniform(5.99, 299.99), 2)
            margin = random.uniform(0.2, 0.6)
            cost_price = round(unit_price * (1 - margin), 2)

            writer.writerow(
                {
                    "product_id": f"PROD-{i:05d}",
                    "product_name": product_name,
                    "category": category,
                    "subcategory": fake.word().capitalize(),
                    "unit_price": unit_price,
                    "cost_price": cost_price,
                    "weight_kg": round(random.uniform(0.1, 15.0), 2),
                    "supplier": fake.company(),
                    "is_active": random.choices([True, False], weights=[90, 10])[0],
                }
            )

    print(f"Generated {NUM_PRODUCTS} products -> {filepath}")


if __name__ == "__main__":
    generate_products()
