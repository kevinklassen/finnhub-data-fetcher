# Description: Fetches profile data for a list of tickers from the Finnhub API
# and saves the data to a CSV file.
import pandas as pd
import os
import asyncio
from modules.fetch_finnhub import fetch_finnhub_data
from modules.plot_api_calls import plot_api_calls_per_minute, plot_api_calls_per_second

# Constants
SIMULTANEOUS_CONNECTIONS = 10
API_DELAY = 2  # 2 seconds
QUERY_MAX = 5
output_folder = "test/data/python"

# Load the Finnhub investable universe
finnhub_universe_df = pd.read_csv("test/data/python/finnhub_investable_universe.csv")
tickers = finnhub_universe_df["Ticker"].tolist()


# Endpoint URL function for profile data
def profile_url_function(ticker, key):
    return f"https://finnhub.io/api/v1/stock/profile?symbol={ticker}&token={key}"


# Fetch the profile data
data_results, api_call_timestamps = asyncio.run(
    fetch_finnhub_data(
        tickers, profile_url_function, SIMULTANEOUS_CONNECTIONS, API_DELAY, QUERY_MAX
    )
)

# Convert the results to a DataFrame
results_df = pd.DataFrame(data_results)

# Save Results to CSV
results_df.to_csv(os.path.join(output_folder, "finnhub_profile.csv"), index=False)

# Save API Call Timestamps to CSV
api_call_timestamps_df = pd.DataFrame(api_call_timestamps)
api_call_timestamps_df.to_csv(
    os.path.join(
        output_folder,
        f"finnhub_profile_api_call_timestamps_{pd.Timestamp.now().date()}.csv",
    ),
    index=False,
)
# Plot API Call Timestamps
plot_api_calls_per_minute(api_call_timestamps)
plot_api_calls_per_second(api_call_timestamps)
