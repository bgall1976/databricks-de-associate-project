"""
Generate synthetic order data as JSON files in batches.
Each batch simulates a new set of files landing in a storage location,
which is the pattern Auto Loader is designed to handle.

Intentionally includes some "dirty" data for quality testing:
- Null values in required fields
- Negative quantities
- Future dates
- Duplicate order IDs (across batches)
- Malformed email addresses in shipping info
"""

import json
import os
import random
from datetime import datetime, timedelta
from faker import Faker

from config import (
    NUM_CUSTOMERS,
    NUM_PRODUCTS,
    NUM_ORDER_BATCHES,
    ORDERS_PER_BATCH,
    ORDERS_DIR,
    PAYMENT_METHODS,
    ORDER_STATUSES,
    RANDOM_SEED,
)

fake = Faker()
Faker.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)

# Track order IDs globally to intentionally create some duplicates
all_order_ids = []


def generate_order_item(order_id: str) -> dict:
    """Generate a single line item within an order."""
    product_id = f"PROD-{random.randint(1, NUM_PRODUCTS):05d}"
    quantity = random.randint(1, 5)

    # Intentionally introduce bad data: ~2% negative quantities
    if random.random() < 0.02:
        quantity = -1 * quantity

    unit_price = round(random.uniform(5.99, 299.99), 2)

    return {
        "product_id": product_id,
        "quantity": quantity,
        "unit_price": unit_price,
        "discount_pct": round(random.uniform(0, 0.3), 2),
        "line_total": round(unit_price * quantity * (1 - random.uniform(0, 0.3)), 2),
    }


def generate_order(batch_num: int, order_num: int) -> dict:
    """Generate a single order as a nested JSON object."""
    order_id = f"ORD-{batch_num:03d}-{order_num:06d}"

    # Intentionally create ~1% duplicate order IDs from previous batches
    if all_order_ids and random.random() < 0.01:
        order_id = random.choice(all_order_ids)
    else:
        all_order_ids.append(order_id)

    customer_id = f"CUST-{random.randint(1, NUM_CUSTOMERS):06d}"

    # Order date: mostly recent, but ~2% have future dates (bad data)
    if random.random() < 0.02:
        order_date = fake.date_time_between(
            start_date="+1d", end_date="+30d"
        )
    else:
        order_date = fake.date_time_between(
            start_date="-90d", end_date="now"
        )

    # Generate 1-5 line items per order
    num_items = random.randint(1, 5)
    items = [generate_order_item(order_id) for _ in range(num_items)]

    order = {
        "order_id": order_id,
        "customer_id": customer_id,
        "order_date": order_date.isoformat(),
        "status": random.choice(ORDER_STATUSES),
        "payment_method": random.choice(PAYMENT_METHODS),
        "shipping_address": {
            "street": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "zip_code": fake.zipcode(),
        },
        "items": items,
        "subtotal": round(sum(item["line_total"] for item in items), 2),
        "tax_amount": round(
            sum(item["line_total"] for item in items) * random.uniform(0.05, 0.1), 2
        ),
        "shipping_cost": round(random.uniform(0, 15.99), 2),
    }

    # Calculate total
    order["total_amount"] = round(
        order["subtotal"] + order["tax_amount"] + order["shipping_cost"], 2
    )

    # Intentionally null out some required fields (~1% of orders)
    if random.random() < 0.01:
        order["customer_id"] = None
    if random.random() < 0.01:
        order["order_date"] = None

    return order


def generate_orders():
    """Generate order JSON files in batches."""
    os.makedirs(ORDERS_DIR, exist_ok=True)

    total_orders = 0
    for batch in range(1, NUM_ORDER_BATCHES + 1):
        batch_orders = []
        for order_num in range(1, ORDERS_PER_BATCH + 1):
            order = generate_order(batch, order_num)
            batch_orders.append(order)

        # Write as newline-delimited JSON (one JSON object per line)
        # This is the format Auto Loader handles most efficiently
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"orders_batch_{batch:03d}_{timestamp}.json"
        filepath = os.path.join(ORDERS_DIR, filename)

        with open(filepath, "w") as f:
            for order in batch_orders:
                f.write(json.dumps(order) + "\n")

        total_orders += len(batch_orders)
        print(f"  Batch {batch}/{NUM_ORDER_BATCHES}: {len(batch_orders)} orders -> {filename}")

    print(f"\nGenerated {total_orders} total orders across {NUM_ORDER_BATCHES} batch files -> {ORDERS_DIR}/")
    print(f"Intentional data quality issues included:")
    print(f"  - ~1% null customer_id or order_date")
    print(f"  - ~2% negative quantities in line items")
    print(f"  - ~2% future order dates")
    print(f"  - ~1% duplicate order IDs across batches")


if __name__ == "__main__":
    generate_orders()
