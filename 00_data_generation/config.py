# Data Generation Configuration

# Number of records to generate
NUM_CUSTOMERS = 500
NUM_PRODUCTS = 100
NUM_ORDER_BATCHES = 10          # Simulates data arriving in batches
ORDERS_PER_BATCH = 1000         # Orders per batch file

# Output directories (local paths - upload to DBFS after generation)
OUTPUT_DIR = "generated_data"
ORDERS_DIR = f"{OUTPUT_DIR}/orders"
CUSTOMERS_DIR = f"{OUTPUT_DIR}/customers"
PRODUCTS_DIR = f"{OUTPUT_DIR}/products"

# Product categories for realistic data
PRODUCT_CATEGORIES = [
    "Electronics",
    "Clothing",
    "Home & Kitchen",
    "Books",
    "Sports & Outdoors",
    "Health & Beauty",
    "Toys & Games",
    "Automotive",
    "Garden & Outdoor",
    "Office Supplies",
]

# US regions for regional analysis
REGIONS = {
    "Northeast": ["NY", "NJ", "PA", "CT", "MA", "NH", "VT", "ME", "RI"],
    "Southeast": ["FL", "GA", "NC", "SC", "VA", "TN", "AL", "MS", "LA", "KY"],
    "Midwest": ["OH", "MI", "IL", "IN", "WI", "MN", "MO", "IA", "KS", "NE"],
    "Southwest": ["TX", "AZ", "NM", "OK"],
    "West": ["CA", "WA", "OR", "CO", "UT", "NV", "HI", "AK", "ID", "MT", "WY"],
}

# Payment methods
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "gift_card"]

# Order statuses (including some that will be "bad" data for quality testing)
ORDER_STATUSES = ["completed", "pending", "shipped", "cancelled", "returned", "refunded"]

# Seed for reproducibility
RANDOM_SEED = 42
