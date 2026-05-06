import sys
import os
sys.path.append('.')

from src.storage.postgres_client import PostgresClient

# Test database connection and watermark functionality
db_config = {
    "host": "localhost",
    "port": 5432,
    "dbname": "finance",
    "user": "postgres",
    "password": "postgres",
}

try:
    with PostgresClient(db_config) as db:
        print("Database connection successful!")
        
        # Test watermark operations
        watermark = db.get_watermark("raw_stock_prices", "AAPL")
        print(f"Current watermark: {watermark}")
        
        # Check if pipeline_state table exists and has data
        result = db.fetch_all("SELECT * FROM pipeline_state LIMIT 5")
        print(f"Pipeline state records: {len(result)}")
        for record in result:
            print(f"  {record}")
            
        print("Test completed successfully!")
        
except Exception as e:
    print(f"Error: {e}")
