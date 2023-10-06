# Import Libraries
import pandas as pd
import requests
from io import StringIO
import os

# Initialize Constants
# Constants
FINNHUB_API_KEY = "c8ob3uaad3iddfsar4r0"
QUERY_MAX = 5
output_folder = "test/data/python"

# List of exchanges to keep
exchange_list = [
    "NASDAQ",
    "New York Stock Exchange Inc.",
    "Nyse Mkt Llc",
    "Cboe BZX formerly known as BATS",
]


# Function to get index data
def get_index_data(url, skip_rows=9):
    response = requests.get(url)
    csv_data = pd.read_csv(StringIO(response.text), skiprows=skip_rows)
    return csv_data[
        (csv_data["Asset Class"] == "Equity") & csv_data["Exchange"].isin(exchange_list)
    ]


# List of index information (name, URL)
index_info_list = [
    (
        "r1000",
        "https://www.ishares.com/us/products/239707/ishares-russell-1000-etf/1467271812596.ajax?fileType=csv&fileName=IWB_holdings&dataType=fund",
    ),
    (
        "r2000",
        "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund",
    ),
    (
        "r3000",
        "https://www.ishares.com/us/products/239714/ishares-russell-3000-etf/1467271812596.ajax?fileType=csv&fileName=IWV_holdings&dataType=fund",
    ),
    (
        "sp500",
        "https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf/1467271812596.ajax?fileType=csv&fileName=IVV_holdings&dataType=fund",
    ),
]

# Ensure output folder exists
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Get and write data for each index
for index_name, index_url in index_info_list:
    # Get data
    index_data = get_index_data(index_url)

    # Write to CSV
    csv_path = os.path.join(output_folder, f"{index_name}.csv")
    index_data.to_csv(csv_path, index=False)
