# Import Libraries
import pandas as pd
import requests
import os

# Constants
FINNHUB_API_KEY = "c8ob3uaad3iddfsar4r0"
output_folder = "test/data/python"

# Step 1: Fetch list of supported tickers
symbols_url = (
    f"https://finnhub.io/api/v1/stock/symbol?exchange=US&token={FINNHUB_API_KEY}"
)
response = requests.get(symbols_url)
symbols_df = pd.DataFrame(response.json())

# Step 2: Write to CSV
csv_path = os.path.join(output_folder, "finnhub_symbol.csv")
