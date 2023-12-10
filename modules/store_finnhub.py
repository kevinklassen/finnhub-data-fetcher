"""store_finnhub - Store data from the Finnhub API.

This module provides functions to store data pulled from the Finnhub API.
"""

from pyspark.sql import SparkSession
from datetime import datetime

# Initialize a Spark session
spark = SparkSession.builder.getOrCreate()


def store_data(data, file_path):
    """
    Writes data to a JSONL file on S3 using spark.

    Parameters:
    - data (list): A list of dictionaries containing data for each ticker.
    - file_path (str): The path from the account-jordanmckinnie-vendor-finnhub
    bucket root where the JSONL file will be saved. The file name will be
    the current date in the format YYYY_MM_DD.

    Returns:
    - None
    """

    # Convert the list of dictionaries to a Spark DataFframe
    data_df = spark.createDataFrame(data)
    print(data_df.show(5))

    # Define the S3 bucket and key for the data
    bucket_name = "account-jordanmckinnie-vendor-finnhub"
    key = f"{file_path}/{datetime.now().strftime('%Y_%m_%d')}.jsonl"

    try:
        # Store the data in S3 using Spark
        data_df.write.option("header", "true").mode("overwrite").json(
            f"./{bucket_name}/{key}"
        )
        print(f"Successfully stored data in S3 at {key}.")
    except Exception as e:
        print(f"Failed to store data in S3 at {key}: {e}")
        raise e
