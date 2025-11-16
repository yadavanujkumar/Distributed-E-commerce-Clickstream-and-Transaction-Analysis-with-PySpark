"""
Data Generation Script for E-commerce Clickstream and Transaction Analysis
Generates simulated datasets: clicks.csv, transactions.json, products.csv
"""

import csv
import json
import random
from datetime import datetime, timedelta

# Configuration
NUM_USERS = 1000
NUM_PRODUCTS = 200
NUM_CLICKS = 50000
NUM_TRANSACTIONS = 10000
OUTPUT_DIR = "data"

# Categories and product names
CATEGORIES = [
    "Electronics", "Clothing", "Home & Kitchen", "Books", 
    "Sports", "Beauty", "Toys", "Automotive"
]

PRODUCT_NAMES = [
    "Wireless Headphones", "Smart Watch", "Laptop", "Tablet", "Phone Case",
    "T-Shirt", "Jeans", "Sneakers", "Dress", "Jacket",
    "Coffee Maker", "Blender", "Toaster", "Vacuum Cleaner", "Air Purifier",
    "Novel", "Cookbook", "Biography", "Science Book", "Magazine",
    "Yoga Mat", "Dumbbells", "Tennis Racket", "Running Shoes", "Bicycle",
    "Shampoo", "Lipstick", "Face Cream", "Perfume", "Nail Polish",
    "Action Figure", "Board Game", "Puzzle", "Doll", "Building Blocks",
    "Car Wax", "Tire Pressure Gauge", "Car Cover", "Floor Mats", "Oil Filter"
]

def generate_products(num_products):
    """Generate products.csv"""
    products = []
    for i in range(1, num_products + 1):
        product = {
            "product_id": i,
            "category": random.choice(CATEGORIES),
            "name": f"{random.choice(PRODUCT_NAMES)} {i}",
            "features": f"Feature_{random.randint(1, 10)}"
        }
        products.append(product)
    
    with open(f"{OUTPUT_DIR}/products.csv", 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["product_id", "category", "name", "features"])
        writer.writeheader()
        writer.writerows(products)
    
    print(f"Generated {num_products} products in {OUTPUT_DIR}/products.csv")
    return products

def generate_clicks(num_clicks, num_users, num_products):
    """Generate clicks.csv with some null user_ids"""
    clicks = []
    base_date = datetime.now() - timedelta(days=60)
    
    for _ in range(num_clicks):
        # Intentionally add some null user_ids (5% of records)
        user_id = random.randint(1, num_users) if random.random() > 0.05 else None
        
        click = {
            "user_id": user_id,
            "product_id": random.randint(1, num_products),
            "timestamp": (base_date + timedelta(
                days=random.randint(0, 60),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )).strftime("%Y-%m-%d %H:%M:%S"),
            "page_views": random.randint(1, 10)
        }
        clicks.append(click)
    
    with open(f"{OUTPUT_DIR}/clicks.csv", 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["user_id", "product_id", "timestamp", "page_views"])
        writer.writeheader()
        writer.writerows(clicks)
    
    print(f"Generated {num_clicks} clicks in {OUTPUT_DIR}/clicks.csv")

def generate_transactions(num_transactions, num_users, num_products):
    """Generate transactions.json"""
    transactions = []
    base_date = datetime.now() - timedelta(days=60)
    
    for _ in range(num_transactions):
        # Intentionally add some invalid prices/quantities (3% of records)
        price = round(random.uniform(5.0, 500.0), 2) if random.random() > 0.03 else random.choice([0, -1.0])
        quantity = random.randint(1, 5) if random.random() > 0.03 else random.choice([0, -1])
        
        transaction = {
            "user_id": random.randint(1, num_users),
            "product_id": random.randint(1, num_products),
            "price": price,
            "quantity": quantity,
            "timestamp": (base_date + timedelta(
                days=random.randint(0, 60),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )).strftime("%Y-%m-%d %H:%M:%S")
        }
        transactions.append(transaction)
    
    with open(f"{OUTPUT_DIR}/transactions.json", 'w') as f:
        for transaction in transactions:
            f.write(json.dumps(transaction) + '\n')
    
    print(f"Generated {num_transactions} transactions in {OUTPUT_DIR}/transactions.json")

def main():
    """Main function to generate all datasets"""
    import os
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print("Starting data generation...")
    generate_products(NUM_PRODUCTS)
    generate_clicks(NUM_CLICKS, NUM_USERS, NUM_PRODUCTS)
    generate_transactions(NUM_TRANSACTIONS, NUM_USERS, NUM_PRODUCTS)
    print("Data generation completed!")

if __name__ == "__main__":
    main()
