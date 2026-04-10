CREATE TABLE IF NOT EXISTS raw_stock_prices (
    symbol TEXT,
    date DATE,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT,
    ingested_at TIMESTAMP DEFAULT NOW(),

    PRIMARY KEY (symbol, date)
);