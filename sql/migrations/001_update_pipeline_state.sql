-- Add symbol column to pipeline_state for per-symbol watermark tracking
-- This migration makes the pipeline scalable for multiple symbols

-- Drop the existing table (we'll recreate it since it's likely empty)
DROP TABLE IF EXISTS pipeline_state;

-- Create the new pipeline_state table with symbol support
CREATE TABLE IF NOT EXISTS pipeline_state (
    table_name TEXT,
    symbol TEXT,
    last_watermark TIMESTAMP,
    updated_at TIMESTAMP DEFAULT NOW(),
    
    PRIMARY KEY (table_name, symbol)
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_pipeline_state_table_symbol ON pipeline_state(table_name, symbol);
