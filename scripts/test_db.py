from src.storage.postgres_client import PostgresClient

config = {
    "host": "localhost",
    "port": 5432,
    "dbname": "finance",
    "user": "postgres",
    "password": "postgres",
}

with PostgresClient(config) as db:
    result = db.fetch_one("SELECT NOW();")
    print(result)