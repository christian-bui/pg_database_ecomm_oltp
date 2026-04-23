import os
from typing import Any, Dict, List
from dotenv import load_dotenv

# Load all variables from the .env file into the system
load_dotenv()

# 1. DATA VOLUME CONTRACTS
# Defines exactly how many rows we want for each table.
DATA_VOLUMES: Dict[str, int] = {
    "brand": 20,
    "category": 10,
    "seller": 25,
    "product": 2000,
    "promotion": 10,
    "promotion_product": 100,
}

# 2. STATIC TAXONOMY (GOOGLE PRODUCT TAXONOMY - GPT)
# Standard category list used for mapping.
CATEGORIES_HIERARCHY: List[Dict[str, Any]] = [
    # Level 1 (Main Categories)
    {"id": 1, "name": "Apparel & Accessories", "parent": None, "level": 1},
    {"id": 2, "name": "Electronics", "parent": None, "level": 1},
    {"id": 3, "name": "Home & Garden", "parent": None, "level": 1},
    # Level 2 (Sub Categories)
    {"id": 4, "name": "Clothing", "parent": 1, "level": 2},
    {"id": 5, "name": "Shoes", "parent": 1, "level": 2},
    {"id": 6, "name": "Computers", "parent": 2, "level": 2},
    {"id": 7, "name": "Communications", "parent": 2, "level": 2},
    {"id": 8, "name": "Audio", "parent": 2, "level": 2},
    {"id": 9, "name": "Kitchen & Dining", "parent": 3, "level": 2},
    {"id": 10, "name": "Furniture", "parent": 3, "level": 2},
]

# 3. GLOBAL DATA STANDARDS
DEFAULT_CURRENCY = "VND"
DEFAULT_COUNTRY_CODE = "VN"
SYSTEM_TIMEZONE = "UTC"

# 4. REPRODUCIBILITY
# This fixed number makes sure our random data is the same every time we run.
RANDOM_SEED = 42

# 5. DATABASE CONFIGURATIONS
# We pull these values from the .env file for security.
# If a value is missing, it uses the default one on the right.
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "ecomm_db")