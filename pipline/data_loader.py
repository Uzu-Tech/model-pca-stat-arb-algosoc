import os
from pathlib import Path

import pandas as pd
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from dotenv import load_dotenv

DATA_PATH = Path("data")
STOCK_DATA_PATH = DATA_PATH / "stock data"

load_dotenv()
EDUFUND_API_KEY = os.getenv("API_KEY")
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")


def load_stock_data(
    tickets: list[str], years_from_2025_end: int, timeframe: TimeFrame
) -> pd.DataFrame:
    file_name = (
        f"stock_data__{len(tickets)}_tickets__{years_from_2025_end}_years__{str(timeframe)}_timeframe.parquet"
    )
    file_path = STOCK_DATA_PATH / file_name

    if file_path.exists():
        df = pd.read_parquet(file_path, engine="pyarrow")
        return df

    client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
    # Consistent end date chosen as end of last year
    end_date = pd.Timestamp("2025-12-31", tz="UTC")
    # Go back one more year for an extension window
    start_date = end_date - pd.DateOffset(years=years_from_2025_end + 1)

    request_params = StockBarsRequest(
        symbol_or_symbols=tickets,
        timeframe=timeframe,
        start=start_date,
        end=end_date,
    )

    print(f"Fetching data from {start_date.date()} to {end_date.date()}...")
    bars = client.get_stock_bars(request_params)
    df = bars.df  # type: ignore  # df property exists at runtime
    df.to_parquet(file_path, engine="pyarrow", compression="snappy")
    return df
