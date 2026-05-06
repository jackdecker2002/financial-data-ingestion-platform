from datetime import datetime, timedelta
import logging
import uuid

from src.ingestion.alpha_vantage_client import AlphaVantageClient
from src.storage.postgres_client import PostgresClient


logging.basicConfig(level=logging.INFO)


def run_stock_ingestion():
    # --- Config ---
    api_key = "RIEPBV7SRRG5ECYS"
    table_name = "raw_stock_prices"

    db_config = {
        "host": "localhost",
        "port": 5432,
        "dbname": "finance",
        "user": "postgres",
        "password": "postgres",
    }

    symbol = "AAPL"
    run_id = str(uuid.uuid4())
    start_time = datetime.now()

    # --- Initialize clients ---
    api_client = AlphaVantageClient(api_key=api_key)

    with PostgresClient(db_config) as db:
        # Log pipeline run start
        db.log_pipeline_run(run_id, start_time, status="running")
        logging.info(f"Starting pipeline run {run_id}")

        try:
            # Get last watermark for incremental ingestion
            last_watermark = db.get_watermark(table_name, symbol)
            
            if last_watermark:
                logging.info(f"Found last watermark for {symbol}: {last_watermark}")
                # Add 1 day to avoid re-fetching the same day
                start_date = last_watermark.date() + timedelta(days=1)
            else:
                logging.info(f"No watermark found for {symbol}, doing full load")
                start_date = None

            logging.info(f"Fetching stock data for {symbol} from {start_date or 'beginning'}")

            df = api_client.get_daily_prices(symbol, start_date=start_date)

            logging.info(f"Fetched {len(df)} rows")

            if df.empty:
                logging.info("No new data returned, exiting")
                end_time = datetime.now()
                db.log_pipeline_run(run_id, start_time, end_time, status="completed")
                return

            logging.info("Inserting into raw_stock_prices")

            db.insert_dataframe(df, table_name)

            # Update watermark to the latest date in the data
            latest_date = df['date'].max()
            db.update_watermark(table_name, symbol, latest_date)
            
            logging.info(f"Updated watermark for {symbol} to {latest_date}")
            logging.info("Ingestion complete")

            # Log successful completion
            end_time = datetime.now()
            db.log_pipeline_run(run_id, start_time, end_time, status="completed")
            logging.info(f"Pipeline run {run_id} completed in {(end_time - start_time).total_seconds():.2f}s")

        except Exception as e:
            logging.error(f"Pipeline failed: {e}")
            # Log failure
            end_time = datetime.now()
            db.log_pipeline_run(run_id, start_time, end_time, status="failed")
            raise


if __name__ == "__main__":
    run_stock_ingestion()