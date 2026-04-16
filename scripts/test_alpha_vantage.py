from src.ingestion.alpha_vantage_client import AlphaVantageClient

client = AlphaVantageClient(api_key="RIEPBV7SRRG5ECYS")

df = client.get_daily_prices("AAPL")

print(df.head())
print(f"Rows fetched: {len(df)}")
