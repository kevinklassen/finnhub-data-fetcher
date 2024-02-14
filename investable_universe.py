################################################################################
# Setup
################################################################################

# Standard library imports
import requests
from io import StringIO
import os

# Third-party imports
import pandas as pd

# Constants
folder = "finnhub"
FINNHUB_API_KEY = 123  # replace with actual key

################################################################################
# Load Supported Tickers
################################################################################

symbols_url = (
    f"https://finnhub.io/api/v1/stock/symbol?exchange=US&token={FINNHUB_API_KEY}"
)
symbols_response = requests.get(symbols_url)
symbols_df = pd.DataFrame(symbols_response.json())

################################################################################
# Load ETF Index Constituents
################################################################################

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


################################################################################
# Add Extra Tickers
################################################################################

# Extra Tickers
extra_tickers = ["HBB", "MMYT", "TGLS", "CYRX", "PROF", "EVLV"]
extra_tickers_df = pd.DataFrame(extra_tickers, columns=["Ticker"])


################################################################################
# Create Investable Universe
################################################################################

# Get the unique indices and create column names with 'in_' prefix
unique_indices = all_indices_df["index_name"].unique()
index_columns = ["in_" + index_name for index_name in unique_indices]

# Initialize the indices columns with 0 for extra tickers
for column in index_columns:
    extra_tickers_df[column] = 0

# Append the extra tickers to all_indices_df
combined_df = pd.concat([all_indices_df, extra_tickers_df], ignore_index=True)

# Convert index_name into dummy variables
index_dummies = pd.get_dummies(
    combined_df[["Ticker", "index_name"]],
    columns=["index_name"],
    prefix="in",
    prefix_sep="_",
)

# Sum up the dummy variables to consolidate ticker information
investable_universe_df = index_dummies.groupby("Ticker").sum().reset_index()

# Filter to keep only the tickers supported by Finnhub
supported_universe_df = investable_universe_df[
    investable_universe_df["Ticker"].isin(symbols_df["symbol"])
]

# Save the Investable Universe to CSV
supported_universe_df.to_csv(
    os.path.join(f"{folder}/datasets/finnhub_investable_universe.csv"), index=False
)
