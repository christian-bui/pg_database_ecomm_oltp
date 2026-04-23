# main.py
from src.logger import get_logger
from src.generator import (
    generate_brands,
    generate_categories,
    generate_sellers,
    generate_products,
    generate_promotions,
    generate_promotion_products
)

# Start the logger to record the process
logger = get_logger("data_pipeline")

def main():
    try:
        logger.info("Starting the E-commerce Synthetic Data Pipeline...")
        
        # 1. Generate basic data tables (these do not depend on others)
        logger.info("Step 1: Generating independent tables (Brand, Category, Seller)")
        df_brand = generate_brands()
        df_category = generate_categories()
        df_seller = generate_sellers()
        
        # Get IDs from the tables above so we can link them to products
        brand_ids = df_brand["brand_id"].to_list()
        category_ids = df_category["category_id"].to_list()
        seller_ids = df_seller["seller_id"].to_list()
        
        # 2. Generate the main products table
        logger.info("Step 2: Generating dependent table (Product)")
        df_product = generate_products(
            brand_ids=brand_ids,
            category_ids=category_ids,
            seller_ids=seller_ids
        )
        
        # 3. Generate promotions and link them to products
        logger.info("Step 3: Generating Promotions and Promo_Products mapping")
        df_promotion = generate_promotions()
        
        df_promo_product = generate_promotion_products(
            promotion_ids=df_promotion["promotion_id"].to_list(),
            product_ids=df_product["product_id"].to_list()
        )
        
        # 4. Final check: Show a summary of all 6 tables currently in RAM
        logger.info("Data Generation Complete. Summary of all 6 tables:")
        logger.info(f"1. Brands        : {df_brand.shape[0]} rows")
        logger.info(f"2. Categories    : {df_category.shape[0]} rows")
        logger.info(f"3. Sellers       : {df_seller.shape[0]} rows")
        logger.info(f"4. Products      : {df_product.shape[0]} rows")
        logger.info(f"5. Promotions    : {df_promotion.shape[0]} rows")
        logger.info(f"6. Promo_Products: {df_promo_product.shape[0]} rows")
        
        # Show one sample schema to verify data types are correct
        logger.info(f"Sample Product Schema: {dict(df_product.schema)}")
        
        logger.info("SUCCESS: All 6 dataframes are ready in memory.")
        logger.info("Next move: Prepare the database to receive this data.")
        
    except Exception as e:
        # If there is an error, record the message and where it happened
        logger.error(f"The pipeline stopped due to an error: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()