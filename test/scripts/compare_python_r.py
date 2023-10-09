import pandas as pd
import numpy as np
import os

# Updated folder paths
r_folder = "test/data/R"
python_folder = "test/data/python"

# List of CSV files to check
csv_files = ["r1000.csv", "r2000.csv", "r3000.csv", "sp500.csv"]


def compare_csv_files(file1, file2):
    """
    Compare two CSV files and provide a reason if they don't match.
    """

    # Read the CSV files into dataframes
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)

    # Initialize a list to hold mismatch reasons
    mismatch_reasons = []

    # Check if both dataframes have the same shape
    if df1.shape != df2.shape:
        mismatch_reasons.append("Shape mismatch")

    # Check if both dataframes have the same columns
    if set(df1.columns) != set(df2.columns):
        mismatch_reasons.append("Column name mismatch")

    # Coerce each pair of columns to a common data type and compare
    common_cols = set(df1.columns).intersection(set(df2.columns))
    mismatch_columns = []
    for col in common_cols:
        try:
            # If the column contains numeric data, convert to float before comparing
            if np.issubdtype(df1[col].dtype, np.number) and np.issubdtype(
                df2[col].dtype, np.number
            ):
                if not pd.Series(df1[col].astype(float)).equals(df2[col].astype(float)):
                    mismatch_columns.append(col)
            else:
                if not pd.Series(df1[col].astype(str)).equals(df2[col].astype(str)):
                    mismatch_columns.append(col)
        except Exception as e:
            mismatch_reasons.append(f"Error in comparing column '{col}': {str(e)}")

    if mismatch_columns:
        mismatch_reasons.append(
            f"Data mismatch in columns: {', '.join(mismatch_columns)}"
        )

    return "Match" if not mismatch_reasons else ", ".join(mismatch_reasons)


# Run the comparison
results = {}
for csv_file in csv_files:
    r_file = os.path.join(r_folder, csv_file)
    python_file = os.path.join(python_folder, csv_file)

    if not os.path.exists(r_file) or not os.path.exists(python_file):
        results[csv_file] = "One of the files does not exist."
    else:
        results[csv_file] = compare_csv_files(r_file, python_file)

# Display the results
for csv_file, match_status in results.items():
    print(f"{csv_file}: {match_status}")
