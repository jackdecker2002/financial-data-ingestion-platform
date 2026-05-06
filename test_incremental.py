#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime
import logging
import uuid

from src.ingestion.alpha_vantage_client import AlphaVantageClient
from src.storage.postgres_client import PostgresClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_incremental():
    print("Testing incremental ingestion...")
    
    # Config
    api_key = "RIEPBV7SRRG5ECYS"
    table_name = "raw_stock_prices"
    symbol = "AAPL"
    
    db_config = {
        "host": "localhost",
        "port": 5432,
        "dbname": "finance",
        "user": "postgres",
        "password": "postgres",
    }
    
    api_client = AlphaVantageClient(api_key=api_key)
    
    with PostgresClient(db_config) as db:
        print("Checking current watermark...")
        last_watermark = db.get_watermark(table_name, symbol)
        print(f"Current watermark: {last_watermark}")
        
        print("Fetching data...")
        if last_watermark:
            start_date = last_watermark.date() + datetime.timedelta(days=1)
            print(f"Fetching from {start_date}")
        else:
            start_date = None
            print("No watermark, doing full load")
        
        df = api_client.get_daily_prices(symbol, start_date=start_date)
        print(f"Fetched {len(df)} rows")
        
        if not df.empty:
            print("Inserting data...")
            db.insert_dataframe(df, table_name)
            
            latest_date = df['date'].max()
            db.update_watermark(table_name, symbol, latest_date)
            print(f"Updated watermark to {latest_date}")
        else:
            print("No new data to insert")
        
        print("Test completed successfully!")

if __name__ == "__main__":
    test_incremental()
