# Description: Script to get the constituents of the ETFs that track the
# Russell 1000, Russell 2000, Russell 3000, and S&P 500 indices
# and save the data to a CSV file.
import pandas as pd
import requests
from io import StringIO
import os

# Initialize Constants
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
        (csv_data["Asset Class"] == "Equity")
        & csv_data["Exchange"].isin(exchange_list)
        & csv_data["Ticker"].str.isalpha()
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

# Create a list to collect each index DataFrame
all_indices_data = []

# Get data for each index
for index_name, index_url in index_info_list:
    # Get data
    index_data = get_index_data(index_url)
    index_data["index_name"] = index_name
    index_data["date"] = pd.Timestamp.now().date()
    index_data["datetime"] = pd.Timestamp.now()

    # Append to the list
    all_indices_data.append(index_data)

# Concatenate all DataFrames
all_indices_df = pd.concat(all_indices_data, ignore_index=True)

# Write combined data to CSV
csv_path = os.path.join(output_folder, "etf_index_constituents.csv")
all_indices_df.to_csv(csv_path, index=False)
