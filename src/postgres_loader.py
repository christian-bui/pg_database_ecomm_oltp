import polars as pl
from src.logger import get_logger
from configs.settings import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

# Start the logger for the loading process
logger = get_logger("postgres_loader")

def get_db_uri() -> str:
    """
    Create the connection string (URL) to find the database.
    Format: postgresql://user:password@host:port/database_name
    """
    return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def load_data_to_postgres(df: pl.DataFrame, table_name: str):
    """
    Push a Polars DataFrame directly into a PostgreSQL table.
    """
    try:
        db_uri = get_db_uri()
        
        logger.info(f"Loading {df.height} rows into the '{table_name}' table...")
        
        # Write data to the database
        # We use if_exists="append" because the tables are already created 
        # by our 01_init_schema.sql script. We just want to add data into them.
        df.write_database(
            table_name=table_name,
            connection=db_uri,
            if_table_exists="append",
            engine="adbc"
        )
        
        logger.info(f"SUCCESS: Data loaded into '{table_name}'.")
        
    except Exception as e:
        logger.error(f"ERROR: Failed to load data into '{table_name}'. Reason: {str(e)}")
        raise