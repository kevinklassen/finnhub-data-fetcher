"""store_finnhub - Store data from the Finnhub API.

This module provides functions to store data pulled from the Finnhub API.
"""

import jsonlines


def write_to_jsonl(data, file_path):
    """
    Writes data to a JSONL file.

    Parameters:
    - data (list): A list of dictionaries containing data for each ticker.
    - file_path (str): The path where the JSONL file will be saved.

    Returns:
    - None
    """
    with jsonlines.open(file_path, mode="w") as writer:
        writer.write_all(data)
