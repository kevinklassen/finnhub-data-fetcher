"""store_finnhub - Store data from the Finnhub API.

This module provides functions to store data pulled from the Finnhub API.
"""

import boto3
import jsonlines
import io


def write_to_jsonl(data, file_path):
    """
    Writes data to a JSONL file on S3.

    Parameters:
    - data (list): A list of dictionaries containing data for each ticker.
    - file_path (str): The S3 key where the JSONL file will be saved in the
    account-jordanmckinnie-vendor-finnhub bucket.

    Returns:
    - None
    """

    # Set up S3 client
    s3 = boto3.client("s3")

    # Write data to in-memory buffer
    buffer = io.BytesIO()
    with jsonlines.Writer(buffer) as writer:
        writer.write_all(data)

    # Upload buffer to S3
    bucket_name = "account-jordanmckinnie-vendor-finnhub"
    s3.upload_fileobj(buffer, bucket_name, file_path)
