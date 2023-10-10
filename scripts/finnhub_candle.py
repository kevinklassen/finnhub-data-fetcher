# Description: Fetches candle data for a list of tickers from the Finnhub API
# and saves the data to a CSV file.
import asyncio
from datetime import datetime, date, timedelta
import os
import pandas as pd
import random
from modules.fetch_finnhub import fetch_finnhub_data
from modules.plot_api_calls import plot_api_calls_per_minute, plot_api_calls_per_second

# Constants
output_folder = "test/data/python"

# Load the Finnhub investable universe
finnhub_universe_df = pd.read_csv("test/data/python/finnhub_investable_universe.csv")
tickers = finnhub_universe_df["Ticker"].tolist()

# Randomize the order of the tickers
random.shuffle(tickers)
tickers = tickers[1:5]

# Fetch the candle data
results_df, api_call_timestamps = asyncio.run(
    fetch_finnhub_data(
        tickers,
        endpoint="candle",
        resolution="D",
        start_date=int(
            datetime.strptime(
                str(date.today() - timedelta(days=7)), "%Y-%m-%d"
            ).timestamp()
        ),
        end_date=int(datetime.strptime(str(date.today()), "%Y-%m-%d").timestamp()),
        simultaneous_connections=2,
        api_delay=1,
    )
)

# Save Results to CSV
results_df.to_csv(os.path.join(output_folder, "finnhub_candle.csv"), index=False)

# Convert results_df t column to datetime
results_df["t"] = pd.to_datetime(results_df["t"], unit="s")

# Save API Call Timestamps to CSV
api_call_timestamps_df = pd.DataFrame(api_call_timestamps)
api_call_timestamps_df.to_csv(
    os.path.join(
        output_folder,
        f"finnhub_candle_api_call_timestamps_{pd.Timestamp.now().date()}.csv",
    ),
    index=False,
)

# Plot API Call Timestamps
plot_api_calls_per_minute(api_call_timestamps)
plot_api_calls_per_second(api_call_timestamps)
