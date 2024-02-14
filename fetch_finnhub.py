"""fetch_finnhub - Fetch data from the Finnhub API.

This module provides functions to asynchronously fetch data from the Finnhub API 
for a given list of tickers. Below are the supported endpoints:
    - "profile": Company Information Data
    - "recommendation": Company Recommendation Data
    - "price-target": Company Price Target Data
    - "revenue-estimate": Sales Estimates
    - "ebitda-estimate": EBITDA Estimate
    - "ebit-estimate": EBIT Estimate
    - "eps-estimate": EPS Estimate
    - "financials": Financial Data
    - "candle": Market Data
Each endpoint may require additional parameters to be provided. Below is a complete
list of possible parameters:
    - statement (str, required for 'financials' endpoint):
        Possible values: 'bs' (Balance Sheet), 'ic' (Income Statement), 'cf' (Cash Flow).
    - freq (str, required for 'financials' and estimates endpoints):
        Possible values: 'annual', 'quarterly', 'ttm' (financials only), and 'ytd' (financials only).
    - resolution (str, required for 'candle' endpoint):
        Possible values: '1', '5', '15', '30', '60', 'D', 'W', 'M'.
    - start_date (int, required for 'candle' endpoint): Start date in UNIX timestamp format.
    - end_date (int, required for 'candle' endpoint): End date in UNIX timestamp format.
The module allows for the use of the API to be configured using the following parameters:
    - simultaneous_connections (int): Maximum number of simultaneous connections.
    - api_delay (int): Delay between API calls in seconds.
    - query_max (int): Maximum number of queries to attempt.
    - key (str, optional): API key for accessing the Finnhub API. Defaults to FINNHUB_API_KEY.

Attributes:
    FINNHUB_API_KEY (str): API key for accessing the Finnhub API.
"""

# Standard library imports
import logging
import random

# Third-party imports
import aiohttp
import asyncio
import pandas as pd

# Initialize logger and spark session
logging.basicConfig(file="finnhub.log", level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
FINNHUB_API_KEY = 123  # replace with actual key
folder = "finnhub"


def get_api_settings(folder):
    """
    Retrieves the Finnhub API connection settings from S3.

    :param str folder: The folder where the API settings are stored.
    :returns: A DataFrame containing the configuration for the Finnhub API.
    :rtype: pandas.DataFrame
    """

    # Set the key for the configuration file
    key = f"configs/api_settings.csv"

    # Read the configuration from S3
    try:
        config_df = pd.read_csv(f"{folder}/{key}")
        logger.info(f"Successfully loaded Finnhub API settings from {folder}/{key}.")
        return config_df

    except Exception as e:
        logger.error(
            f"Failed to load Finnhub API settings from {folder}/{key}: {e}",
            exc_info=True,
        )
        raise e


def get_endpoint_parameters(folder):
    """
    Retrieves the Finnhub endpoint parameters from S3.

    :param str folder: The folder where the endpoint parameters are stored.
    :returns: A DataFrame containing the configuration for the Finnhub API endpoints.
    :rtype: pandas.DataFrame
    """

    # Set the key for the configuration file
    key = f"configs/endpoint_parameters.csv"

    # Read the configuration from S3
    try:
        config_df = pd.read_csv(f"{folder}/{key}")
        logger.info(
            f"Successfully loaded Finnhub endpoint parameters from {folder}/{key}."
        )
        return config_df

    except Exception as e:
        logger.error(
            f"Failed to load Finnhub API endpoint parameters from {folder}/{key}: {e}",
            exc_info=True,
        )
        raise e


def get_endpoint_data_keys(folder):
    """
    Retrieves the Finnhub endpoint data keys, including JSON key data is stored and primary key
    to merge on from S3.

    :param str folder: The folder where the endpoint data keys are stored.
    :returns: A DataFrame containing the configuration for the Finnhub API endpoints.
    :rtype: pandas.DataFrame
    """

    # Set the key for the configuration file
    key = f"configs/endpoint_data_keys.csv"

    # Read the configuration from S3
    try:
        config_df = pd.read_csv(f"{folder}/{key}")
        logger.info(
            f"Successfully loaded Finnhub endpoint parameters from {folder}/{key}."
        )
        return config_df

    except Exception as e:
        logger.error(
            f"Failed to load Finnhub API endpoint parameters from {folder}/{key}: {e}",
            exc_info=True,
        )
        raise e


# Load configuration
API_SETTINGS = get_api_settings(folder)
ENDPOINT_PARAMETERS = get_endpoint_parameters(folder)
ENDPOINT_DATA_KEYS = get_endpoint_data_keys(folder)


def get_endpoint_config(endpoint, sub_endpoint, **kwargs):
    """
    Load API settings and parameters for a given endpoint and/or sub_endpoint.

    :param str endpoint: The name of the endpoint.
    :param sub_endpoint: The name of the sub_endpoint, defaults to None.
    :type sub_endpoint: str or None
    :param kwargs: Additional parameters to override the config parameters.
    :type kwargs: dict or None
    :return: A dictionary with "api" containing the API settings, "params" containing the parameters, and "data_keys" containing the data keys.
    :rtype: dict
    """

    # Filter for default settings
    default_settings_df = API_SETTINGS[
        (API_SETTINGS["endpoint"] == "default") & (API_SETTINGS["sub_endpoint"].isna())
    ]
    default_settings = {
        row["setting"]: row["value"] for _, row in default_settings_df.iterrows()
    }

    # Filter for endpoint-specific settings
    endpoint_settings_df = API_SETTINGS[
        (API_SETTINGS["endpoint"] == endpoint) & (API_SETTINGS["sub_endpoint"].isna())
    ]
    endpoint_settings = {
        row["setting"]: row["value"] for _, row in endpoint_settings_df.iterrows()
    }

    # Override default settings with endpoint-specific settings
    combined_settings = {**default_settings, **endpoint_settings}

    # Filter for sub_endpoint-specific settings
    sub_endpoint_settings_df = API_SETTINGS[
        (API_SETTINGS["endpoint"] == endpoint)
        & (API_SETTINGS["sub_endpoint"] == sub_endpoint)
    ]
    sub_endpoint_settings = {
        row["setting"]: row["value"] for _, row in sub_endpoint_settings_df.iterrows()
    }

    # Override combined settings with sub_endpoint-specific settings
    api_settings = {**combined_settings, **sub_endpoint_settings}

    # Filter for endpoint-specific parameters
    endpoint_params_df = ENDPOINT_PARAMETERS[
        (ENDPOINT_PARAMETERS["endpoint"] == endpoint)
        & (ENDPOINT_PARAMETERS["sub_endpoint"] == sub_endpoint)
    ]
    endpoint_params = {
        row["parameter"]: row["value"] for _, row in endpoint_params_df.iterrows()
    }

    # If no sub_endpoint-specific parameters, try to get endpoint-specific parameters
    if endpoint_params_df.empty and sub_endpoint:
        endpoint_params_df = ENDPOINT_PARAMETERS[
            (ENDPOINT_PARAMETERS["endpoint"] == endpoint)
            & (ENDPOINT_PARAMETERS["sub_endpoint"].isna())
        ]
        endpoint_params = {
            row["parameter"]: row["value"] for _, row in endpoint_params_df.iterrows()
        }

    # Override any params values with values from kwargs
    endpoint_params.update(kwargs)

    # Filter for endpoint-specific data keys
    data_keys_df = ENDPOINT_DATA_KEYS[ENDPOINT_DATA_KEYS["endpoint"] == endpoint]
    if not data_keys_df.empty:
        data_keys = {
            "data_json_key": data_keys_df.iloc[0]["data_json_key"],
            "primary_key": data_keys_df.iloc[0]["primary_key"],
        }
    else:
        data_keys = {"data_json_key": None, "primary_key": None}

    # Override any params values with values from kwargs
    endpoint_params.update(kwargs)

    # Combine API settings, parameters, and data keys
    config = {"api": api_settings, "params": endpoint_params, "data_keys": data_keys}

    return config


def get_endpoint_url_function(endpoint, params):
    """
    Returns a function to generate the URL for the specified Finnhub endpoint.

    :param str endpoint: The name of the Finnhub endpoint.
    :param dict params: Configuration dictionary containing the params for the endpoint.
    :return: A function that, when called with a ticker and an API key, returns the full URL for the API request.
    :rtype: function
    """

    # Get the parameters for the endpoint and construct the string
    param_str = "&".join([f"{param}={{{param}}}" for param in params])

    def url_function(ticker, key):
        # Construct the URL with the parameters
        base_url = f"https://finnhub.io/api/v1/stock/{endpoint}?{param_str}&symbol={ticker}&token={key}"
        return base_url.format(**params)

    return url_function


async def fetch_data_for_ticker(
    ticker,
    api_key,
    endpoint_url_function,
    semaphore,
    api_settings,
    data_keys,
):
    """
    Asynchronously fetches Finnhub data for a given ticker.

    :param str ticker: Stock ticker symbol.
    :param str api_key: API key for accessing the Finnhub API.
    :param function endpoint_url_function: Function to generate endpoint URL.
    :param asyncio.Semaphore semaphore: Semaphore to control concurrent requests.
    :param dict api_settings: Configuration dictionary containing 'api_delay' and 'query_max' values.
    :param dict data_keys: Configuration dictionary containing the data keys for the endpoint.
    :return: A DataFrame containing the data for the ticker.
    :rtype: pd.DataFrame
    """

    # Initialize required variables
    api_delay = int(api_settings.get("api_delay"))
    query_max = int(api_settings.get("query_max"))
    data_json_key = data_keys.get("data_json_key")

    async with semaphore:
        for _ in range(query_max):
            try:
                async with aiohttp.ClientSession() as session:
                    # Fetch the data
                    full_url = endpoint_url_function(ticker, api_key)
                    async with session.get(full_url, timeout=10) as response:
                        # Check if the API call was successful
                        response.raise_for_status()
                        # Delay the next API call
                        await asyncio.sleep(api_delay)
                        # Parse the response
                        data = await response.json()
                        if data:
                            # Create a DataFrame from the data
                            output = pd.DataFrame(data[data_json_key])
                            output["ticker"] = ticker
                            return output
            except aiohttp.ClientError as e:
                logger.error(f"Error fetching data for {ticker}: {e}")

        return


async def fetch_data_for_tickers(
    tickers,
    api_key,
    endpoint_url_function,
    api_settings,
    data_keys,
):
    """
    Asynchronously fetches Finnhub data for a list of tickers.

    :param list tickers: List of stock ticker symbols.
    :param str api_key: API key for accessing the Finnhub API.
    :param function endpoint_url_function: Function to generate endpoint URLs.
    :param dict api_settings: Configuration dictionary containing 'simultaneous_connections', 'api_delay', and 'query_max' values.
    :param dict data_keys: Configuration dictionary containing the data keys for the endpoint.
    :return: A list of dictionaries, each containing data for one ticker.
             Each dictionary has fields for the ticker, timestamp, and the data itself.
             Missing data is indicated by a None value in the 'data' field.
    :rtype: list
    """

    # Initialize semaphore for concurrent requests
    semaphore = asyncio.Semaphore(int(api_settings.get("simultaneous_connections")))

    # Fetch data for each ticker and gather successes and failures
    results = await asyncio.gather(
        *(
            fetch_data_for_ticker(
                ticker,
                api_key,
                endpoint_url_function,
                semaphore,
                api_settings,
                data_keys,
            )
            for ticker in tickers
        )
    )

    return pd.concat(results, ignore_index=True)


def fetch_data_for_endpoint(endpoint, sub_endpoint=None, tickers=None, **kwargs):
    """
    Fetches data from a specified Finnhub endpoint for all tickers in the investable universe or provided tickers.

    :param str endpoint: The name of the Finnhub endpoint.
    :param str sub_endpoint: (optional) The name of the Finnhub sub-endpoint such as "bs_annual" within the "financials" endpoint.
    :param list tickers: (optional) List of stock ticker symbols to fetch data for.
    :param dict kwargs: Additional parameters to override the config parameters.
    :return: A list of dictionaries, each containing data for one ticker.
             Each dictionary has fields for the ticker, timestamp, and the data itself.
             Missing data is indicated by a None value in the 'data' field.
    :rtype: list
    """

    # Get the configuration and URL for the endpoint
    config = get_endpoint_config(endpoint, sub_endpoint, **kwargs)
    endpoint_url_function = get_endpoint_url_function(
        endpoint, params=config.get("params")
    )

    # Get the storage key for the endpoint
    if sub_endpoint:
        data_file_name = f"{endpoint}_{sub_endpoint}"
    else:
        data_file_name = endpoint

    # Load the Finnhub investable universe if tickers are not provided
    if tickers is None:
        finnhub_universe_df = pd.read_csv(
            f"{folder}/datasets/finnhub_investable_universe.csv"
        )
        tickers = finnhub_universe_df["Ticker"].tolist()
        random.shuffle(tickers)  # Randomize the order of the tickers

    # Fetch from Finnhub API
    data = asyncio.run(
        fetch_data_for_tickers(
            tickers=tickers,
            api_key=FINNHUB_API_KEY,
            endpoint_url_function=endpoint_url_function,
            api_settings=config.get("api"),
            data_keys=config.get("data_keys"),
        )
    )

    # Store data to folder
    data.to_csv(f"{folder}/datasets/{data_file_name}.csv", index=False)

    return data
