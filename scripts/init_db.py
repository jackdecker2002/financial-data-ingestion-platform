from src.storage.postgres_client import PostgresClient

def run_schema(db, path):
    with open(path, "r") as f:
        db.execute(f.read())

config = {...}

with PostgresClient(config) as db:
    run_schema(db, "sql/raw/raw_stock_prices.sql")
    run_schema(db, "sql/raw/raw_news.sql")
    run_schema(db, "sql/metadata/pipeline_state.sql")
    run_schema(db, "sql/metadata/pipeline_runs.sql")
    run_schema(db, "sql/metadata/table_metrics.sql")