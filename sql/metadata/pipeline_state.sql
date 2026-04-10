CREATE TABLE IF NOT EXISTS pipeline_state (
    table_name TEXT PRIMARY KEY,
    last_watermark TIMESTAMP,
    updated_at TIMESTAMP DEFAULT NOW()
);