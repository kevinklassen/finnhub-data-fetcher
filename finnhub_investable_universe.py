# Description: Creates a CSV file containing the investable universe of stocks
# that are members of the Russell 1000, Russell 2000, Russell 3000, and S&P 500
# indices. Extra tickers are also included. The CSV file also contains flags
# indicating whether each ticker is a member of each index.
import pandas as pd
import os

# Constants
output_folder = "test/data/python"

# Load Supported Tickers
symbols_df = pd.read_csv(os.path.join(output_folder, "finnhub_symbol.csv"))

# Load ETF Index Constituents
all_indices_df = pd.read_csv(os.path.join(output_folder, "etf_index_constituents.csv"))

# Extra Tickers
extra_tickers = ["HBB", "MMYT", "TGLS", "CYRX", "PROF", "EVLV"]
extra_tickers_df = pd.DataFrame(extra_tickers, columns=["Ticker"])

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
csv_path = os.path.join(output_folder, "finnhub_investable_universe.csv")
supported_universe_df.to_csv(csv_path, index=False)
