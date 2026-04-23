import random
from datetime import timedelta

import polars as pl
from faker import Faker

# Get settings from our main config file
from configs.settings import (
    CATEGORIES_HIERARCHY,
    DATA_VOLUMES,
    DEFAULT_COUNTRY_CODE,
    RANDOM_SEED,
    SYSTEM_TIMEZONE,
)

# Setup tools to generate synthetic data.
# Using a fixed seed ensures we get the exact same data every time we run this code.
fake = Faker()
Faker.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)


def generate_brands() -> pl.DataFrame:
    """Generate synthetic brand data and set correct data types for the database."""
    num_rows = DATA_VOLUMES["brand"]
    data = [
        {
            "brand_id": i,
            "brand_name": fake.company(),
            "country": DEFAULT_COUNTRY_CODE,
            "created_at": fake.date_time_this_decade(),
        }
        for i in range(1, num_rows + 1)
    ]

    # Force Polars to use specific data types before saving to database
    return pl.DataFrame(data).with_columns(
        [
            pl.col("brand_id").cast(pl.Int32),
            pl.col("brand_name").cast(pl.String),
            pl.col("country").cast(pl.String),
            pl.col("created_at").cast(pl.Datetime("us", SYSTEM_TIMEZONE)),
        ]
    )


def generate_categories() -> pl.DataFrame:
    """Load the fixed list of categories from settings and format them."""
    df = pl.DataFrame(CATEGORIES_HIERARCHY)

    # Rename columns to match the database table and set data types
    return df.with_columns(
        [
            pl.col("id").cast(pl.Int32).alias("category_id"),
            pl.col("name").cast(pl.String).alias("category_name"),
            pl.col("parent").cast(pl.Int32).alias("parent_category_id"),
            pl.col("level").cast(pl.Int32),
            pl.lit(fake.date_time_this_year())
            .alias("created_at")
            .cast(pl.Datetime("us", SYSTEM_TIMEZONE)),
        ]
    ).select(
        ["category_id", "category_name", "parent_category_id", "level", "created_at"]
    )


def generate_sellers() -> pl.DataFrame:
    """Generate synthetic seller profiles using standard country codes."""
    num_rows = DATA_VOLUMES["seller"]
    seller_types = ["Official", "Marketplace"]

    data = [
        {
            "seller_id": i,
            "seller_name": f"{fake.company()} VN",
            "join_date": fake.date_between(start_date="-3y", end_date="today"),
            "seller_type": random.choice(seller_types),
            "rating": round(random.uniform(3, 5), 1),
            "country": DEFAULT_COUNTRY_CODE,
        }
        for i in range(1, num_rows + 1)
    ]

    return pl.DataFrame(data).with_columns(
        [
            pl.col("seller_id").cast(pl.Int32),
            pl.col("seller_name").cast(pl.String),
            pl.col("join_date").cast(pl.Date),
            pl.col("seller_type").cast(pl.String),
            pl.col("rating").cast(pl.Float32),
            pl.col("country").cast(pl.String),
        ]
    )


def generate_products(
    brand_ids: list, category_ids: list, seller_ids: list
) -> pl.DataFrame:
    """Generate mock products and link them to existing brands, categories, and sellers."""
    num_rows = DATA_VOLUMES["product"]
    data = []

    for i in range(1, num_rows + 1):
        price = round(random.uniform(100000, 50000000), 2)
        discount_price = round(price * random.uniform(0.7, 1.0), 2)

        data.append(
            {
                "product_id": i,
                "product_name": fake.catch_phrase(),
                "category_id": random.choice(category_ids),
                "brand_id": random.choice(brand_ids),
                "seller_id": random.choice(seller_ids),
                "price": price,
                "discount_price": discount_price,
                "stock_qty": random.randint(0, 500),
                "rating": round(random.uniform(3, 5), 1),
                "created_at": fake.date_time_between(start_date="-3y", end_date="now"),
                "is_active": fake.boolean(),
            }
        )

    return pl.DataFrame(data).with_columns(
        [
            pl.col("product_id").cast(pl.Int32),
            pl.col("product_name").cast(pl.String),
            pl.col("category_id").cast(pl.Int32),
            pl.col("brand_id").cast(pl.Int32),
            pl.col("seller_id").cast(pl.Int32),
            pl.col("price").cast(pl.Float64),
            pl.col("discount_price").cast(pl.Float64),
            pl.col("stock_qty").cast(pl.Int32),
            pl.col("rating").cast(pl.Float32),
            pl.col("created_at").cast(pl.Datetime("us", SYSTEM_TIMEZONE)),
            pl.col("is_active").cast(pl.Boolean),
        ]
    )


def generate_promotions() -> pl.DataFrame:
    """Generate synthetic discount events (promotions)."""
    num_rows = DATA_VOLUMES["promotion"]
    promo_types = ["product", "category", "seller", "flash_sale"]
    discount_types = ["percentage", "fixed_amount"]
    data = []

    for i in range(1, num_rows + 1):
        discount_type = random.choice(discount_types)

        # Calculate discount based on its type
        if discount_type == "percentage":
            discount_val = round(random.uniform(5.0, 50.0), 2)
        else:
            discount_val = round(random.uniform(10000, 500000), 2)

        start_dt = fake.date_between(start_date="-1y", end_date="+1y")
        # Ensure the end date is 30 to 50 days after the start date
        end_dt = start_dt + timedelta(days=random.randint(30, 50))

        data.append(
            {
                "promotion_id": i,
                "promotion_name": f"{fake.word().capitalize()} Mega Sale",
                "promotion_type": random.choice(promo_types),
                "discount_type": discount_type,
                "discount_value": discount_val,
                "start_date": start_dt,
                "end_date": end_dt,
            }
        )

    return pl.DataFrame(data).with_columns(
        [
            pl.col("promotion_id").cast(pl.Int32),
            pl.col("promotion_name").cast(pl.String),
            pl.col("promotion_type").cast(pl.String),
            pl.col("discount_type").cast(pl.String),
            pl.col("discount_value").cast(pl.Float64),
            pl.col("start_date").cast(pl.Date),
            pl.col("end_date").cast(pl.Date),
        ]
    )


def generate_promotion_products(promotion_ids: list, product_ids: list) -> pl.DataFrame:
    """Link promotions to products without creating duplicates."""
    num_rows = DATA_VOLUMES["promotion_product"]
    unique_pairs = set()

    # Keep picking random pairs until we reach the exact number we need
    while len(unique_pairs) < num_rows:
        unique_pairs.add((random.choice(promotion_ids), random.choice(product_ids)))

    data = [
        {
            "promo_product_id": i,
            "promotion_id": promo_id,
            "product_id": prod_id,
            "created_at": fake.date_time_this_year(),
        }
        for i, (promo_id, prod_id) in enumerate(unique_pairs, start=1)
    ]

    return pl.DataFrame(data).with_columns(
        [
            pl.col("promo_product_id").cast(pl.Int32),
            pl.col("promotion_id").cast(pl.Int32),
            pl.col("product_id").cast(pl.Int32),
            pl.col("created_at").cast(pl.Datetime("us", SYSTEM_TIMEZONE)),
        ]
    )
