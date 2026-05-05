from datetime import datetime
import logging

from src.ingestion.alpha_vantage_client import AlphaVantageClient
from src.storage.postgres_client import PostgresClient


logging.basicConfig(level=logging.INFO)


def run_stock_ingestion():
    # --- Config ---
    api_key = "RIEPBV7SRRG5ECYS"

    db_config = {
        "host": "localhost",
        "port": 5432,
        "dbname": "finance",
        "user": "postgres",
        "password": "postgres",
    }

    symbol = "AAPL"

    # --- Initialize clients ---
    api_client = AlphaVantageClient(api_key=api_key)

    with PostgresClient(db_config) as db:
        logging.info(f"Fetching stock data for {symbol}")

        df = api_client.get_daily_prices(symbol)

        logging.info(f"Fetched {len(df)} rows")

        if df.empty:
            logging.info("No data returned, exiting")
            return

        logging.info("Inserting into raw_stock_prices")

        db.insert_dataframe(df, "raw_stock_prices")

        logging.info("Ingestion complete")


if __name__ == "__main__":
    run_stock_ingestion()