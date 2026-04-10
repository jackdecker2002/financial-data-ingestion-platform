CREATE TABLE IF NOT EXISTS raw_news (
    id SERIAL PRIMARY KEY,
    symbol TEXT,
    title TEXT,
    source TEXT,
    published_at TIMESTAMP,
    url TEXT,
    ingested_at TIMESTAMP DEFAULT NOW()
);