import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import pandas as pd
import logging

class PostgresClient:
    def __init__(self, config: dict):
        self.config = config
        self.conn = None

    def _connect(self):
        return psycopg2.connect(
            host=self.config["host"],
            port=self.config["port"],
            dbname=self.config["dbname"],
            user=self.config["user"],
            password=self.config["password"],
        )
    
    def _ensure_connection(self):
        if self.conn is None or self.conn.closed != 0:
            self.conn = self._connect()

    def execute(self, query: str, params: tuple = None):
        try:
            self._ensure_connection()
            with self.conn.cursor() as cur:
                cur.execute(query, params)
            self.conn.commit()
        except Exception as e:
            logging.error(f"Query failed: {e}")
            self.conn.rollback()
            raise

    def __enter__(self):
        self._ensure_connection()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            logging.error(f"Exception occurred: {exc_val}")
        self.close()

    def fetch_one(self, query: str, params: tuple = None):
        try:
            self._ensure_connection()
            with self.conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchone()
        except Exception as e:
            logging.error(f"Fetch one failed: {e}")
            self.conn.rollback()
            raise

    def fetch_all(self, query: str, params: tuple = None):
        try:
            self._ensure_connection()
            with self.conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall()
        except Exception as e:
            logging.error(f"Fetch all failed: {e}")
            self.conn.rollback()
            raise

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

        try:
            self._ensure_connection()
            with self.conn.cursor() as cur:
                execute_values(cur, query.as_string(self.conn), values)
            self.conn.commit()
        except Exception as e:
            logging.error(f"Insert dataframe failed: {e}")
            self.conn.rollback()
            raise

    def close(self):
        if self.conn and self.conn.closed == 0:
            self.conn.close()