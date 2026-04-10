CREATE TABLE IF NOT EXISTS table_metrics (
    run_id TEXT,
    table_name TEXT,
    rows_inserted INT,
    api_latency_ms INT,
    status TEXT
);