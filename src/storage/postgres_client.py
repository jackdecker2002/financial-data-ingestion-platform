import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import pandas as pd
import logging

class PostgresClient:
    def __init__(self, config: dict):
        self.config = config
        self.conn = self._connect()

    def _connect(self):
        return psycopg2.connect(
            host=self.config["host"],
            port=self.config["port"],
            dbname=self.config["dbname"],
            user=self.config["user"],
            password=self.config["password"],
        )

    def execute(self, query: str, params: tuple = None):
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params)
            self.conn.commit()
        except Exception as e:
            logging.error(f"Query failed: {e}")
            self.conn.rollback()
            raise

    def fetch_one(self, query: str, params: tuple = None):
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchone()

    def fetch_all(self, query: str, params: tuple = None):
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def insert_dataframe(self, df: pd.DataFrame, table_name: str):
        if df.empty:
            return

        columns = list(df.columns)
        values = [tuple(row) for row in df.to_numpy()]

        query = sql.SQL(
            "INSERT INTO {table} ({fields}) VALUES %s ON CONFLICT DO NOTHING"
        ).format(
            table=sql.Identifier(table_name),
            fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
        )

        with self.conn.cursor() as cur:
            # execute_values accepts a query string, so convert the SQL object to string
            execute_values(cur, query.as_string(self.conn), values)

        self.conn.commit()

    def close(self):
        self.conn.close()