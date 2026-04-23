import polars as pl
import sqlalchemy
from src.logger import get_logger
from src.generator import (
    generate_brands,
    generate_categories,
    generate_sellers,
    generate_products,
    generate_promotions,
    generate_promotion_products
)
from src.postgres_loader import load_data_to_postgres, get_db_uri

logger = get_logger("data_pipeline")


def reset_database():
    """
    Clear old data from all tables before loading new data.
    This prevents duplicate records if we run the script twice.
    """
    db_uri = get_db_uri()
    engine = sqlalchemy.create_engine(db_uri)
    
    try:
        with engine.begin() as conn:
            logger.info("Clearing old data from tables (TRUNCATE CASCADE)...")
            
            # Wrap the raw SQL string with sqlalchemy.text()
            # CASCADE deletes linked records automatically to avoid errors.
            conn.execute(sqlalchemy.text("""
                TRUNCATE TABLE promotion_products, promotions, products, sellers, categories, brands CASCADE;
            """))
    except Exception as e:
        logger.warning(f"Could not clear tables (they might be empty). Details: {e}")
    finally:
        engine.dispose()


def main():
    try:
        logger.info("Starting the End-to-End E-commerce Data Pipeline...")
        
        # 0. Clean the database before running
        reset_database()
        
        # 1. Generate and load independent tables
        logger.info("--- Stage 1: Processing Independent Tables ---")
        
        df_brand = generate_brands()
        load_data_to_postgres(df_brand, "brands")
        brand_ids = df_brand["brand_id"].to_list()
        del df_brand  # Free up RAM
        
        df_category = generate_categories()
        load_data_to_postgres(df_category, "categories")
        category_ids = df_category["category_id"].to_list()
        del df_category
        
        df_seller = generate_sellers()
        load_data_to_postgres(df_seller, "sellers")
        seller_ids = df_seller["seller_id"].to_list()
        del df_seller
        
        # 2. Generate and load dependent table (Products)
        logger.info("--- Stage 2: Processing Dependent Table (Products) ---")
        df_product = generate_products(brand_ids, category_ids, seller_ids)
        
        # Fix data types for database compatibility
        # We must cast float numbers to strings first, then to decimals.
        # This prevents losing small fractions during the conversion.
        df_product = df_product.with_columns([
            pl.col("price").cast(pl.String).cast(pl.Decimal(scale=2)),
            pl.col("discount_price").cast(pl.String).cast(pl.Decimal(scale=2))
        ])
        
        load_data_to_postgres(df_product, "products")
        product_ids = df_product["product_id"].to_list()
        del df_product
        
        # Delete lists that are no longer needed to save memory
        del brand_ids, category_ids, seller_ids
        
        # 3. Generate and load promotions and mappings
        logger.info("--- Stage 3: Processing Promotions & Mapping ---")
        df_promotion = generate_promotions()
        
        # Fix data type for the discount value column to match the database
        df_promotion = df_promotion.with_columns(
            pl.col("discount_value").cast(pl.String).cast(pl.Decimal(scale=2))
        )
        
        load_data_to_postgres(df_promotion, "promotions")
        promotion_ids = df_promotion["promotion_id"].to_list()
        del df_promotion
        
        df_promo_product = generate_promotion_products(promotion_ids, product_ids)
        load_data_to_postgres(df_promo_product, "promotion_products")
        
        # Final cleanup
        del df_promo_product, promotion_ids, product_ids
        
        logger.info("SUCCESS: All data successfully loaded to PostgreSQL.")
        
    except Exception as e:
        logger.error(f"Pipeline crashed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()