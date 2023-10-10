# Description: Fetches EPS estimate data for a list of tickers from the
# Finnhub API and saves the data to a CSV file.
import asyncio
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

# Fetch the EPS estimates data
results_df, api_call_timestamps = asyncio.run(
    fetch_finnhub_data(
        tickers,
        endpoint="eps-estimate",
    )
)

# Save Results to CSV
results_df.to_csv(os.path.join(output_folder, "finnhub_eps_estimate.csv"), index=False)

# Save API Call Timestamps to CSV
api_call_timestamps_df = pd.DataFrame(api_call_timestamps)
api_call_timestamps_df.to_csv(
    os.path.join(
        output_folder,
        f"finnhub_eps_estimate_api_call_timestamps_{pd.Timestamp.now().date()}.csv",
    ),
    index=False,
)

# Plot API Call Timestamps
plot_api_calls_per_minute(api_call_timestamps)
plot_api_calls_per_second(api_call_timestamps)
