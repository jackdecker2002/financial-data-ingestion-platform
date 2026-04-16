import requests
import pandas as pd
import logging
from datetime import datetime


class AlphaVantageClient:
    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self, api_key: str):
        self.api_key = api_key

    def get_daily_prices(self, symbol: str, start_date: datetime = None) -> pd.DataFrame:
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": self.api_key,
            "outputsize": "compact"
        }

        try:
            response = requests.get(self.BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()

        except Exception as e:
            logging.error(f"API request failed for {symbol}: {e}")
            raise

        if "Error Message" in data or "Information" in data:
            raise ValueError(f"API error for {symbol}: {data}")

        time_series = data.get("Time Series (Daily)")

        if not time_series:
            raise ValueError(f"No time series data returned for {symbol}: {data}")

        rows = []

        for date_str, values in time_series.items():
            try:
                date = datetime.strptime(date_str, "%Y-%m-%d")

                if start_date and date < start_date:
                    continue

                rows.append({
                    "symbol": symbol,
                    "date": date,
                    "open": float(values["1. open"]),
                    "high": float(values["2. high"]),
                    "low": float(values["3. low"]),
                    "close": float(values["4. close"]),
                    "volume": int(values["5. volume"]),
                })

            except Exception as e:
                logging.warning(f"Skipping bad record for {symbol} on {date_str}: {e}")
                continue

        df = pd.DataFrame(rows)

        return df