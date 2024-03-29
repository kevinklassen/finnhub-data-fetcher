"""Module for fetching data from the Finnhub API asynchronously.

This module provides functionality to asynchronously fetch data from various endpoints of the Finnhub API for a given list of stock tickers. It supports fetching data from endpoints such as company profiles, financials, market data, and estimates among others. Each endpoint may require specific parameters which can be configured as needed.

The module allows configuring API usage parameters such as the number of simultaneous connections, delay between API calls, and maximum number of query attempts. It also supports reading API settings and endpoint parameters from configuration files stored in an S3 bucket.

Functions:
    get_index_data(url, exchange_list, skip_rows=9): Fetches and filters index data from a given URL.
    create_investable_universe(extra_tickers): Creates an investable universe of tickers based on specified indices and extra tickers.
    get_endpoint_config(endpoint, sub_endpoint, **kwargs): Loads API settings and parameters for a given endpoint.
    get_endpoint_url_function(endpoint, params): Returns a function to generate the URL for a specified endpoint.
    fetch_data_for_ticker(ticker, api_key, endpoint_url_function, semaphore, api_settings, data_keys): Asynchronously fetches data for a single ticker.
    fetch_data_for_tickers(tickers, api_key, endpoint_url_function, api_settings, data_keys): Asynchronously fetches data for multiple tickers.
    fetch_data_for_endpoint(endpoint, sub_endpoint=None, tickers=None, **kwargs): Fetches data from a specified endpoint for provided tickers.

Examples:
    To fetch company profile data for the investable universe of stocks:
    >>> fetch_data_for_endpoint(endpoint="profile", api_key="your api key")

    To fetch annual balance sheet data for a specific list of tickers:
    >>> fetch_data_for_endpoint(endpoint="financials", sub_endpoint="bs_annual", tickers=["AAPL", "MSFT", "GOOGL"], api_key="your api key")
"""

# Standard library imports
import asyncio
import datetime
from io import StringIO
import logging
import random
import requests
import os

# Third-party imports
import aiohttp
import pandas as pd

# Initialize logger and spark session
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("finnhub.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Load configuration files
API_SETTINGS = pd.read_csv("finnhub/configs/api_settings.csv")
ENDPOINT_PARAMETERS = pd.read_csv("finnhub/configs/endpoint_parameters.csv")
ENDPOINT_DATA_KEYS = pd.read_csv("finnhub/configs/endpoint_data_keys.csv")


def get_index_data(
    url,
    exchange_list,
    skip_rows=9,
):
    """
    Fetches and filters index data from a given URL.

    :param str url: The URL from which to fetch the CSV data.
    :param list exchange_list: A list of exchanges to filter the tickers by.
    :param int skip_rows: The number of rows to skip at the start of the CSV file, defaults to 9.
    :return: A DataFrame containing the filtered index data.
    :rtype: pandas.DataFrame
    """

    response = requests.get(url)
    csv_data = pd.read_csv(StringIO(response.text), skiprows=skip_rows)
    return csv_data[
        (csv_data["Asset Class"] == "Equity")
        & csv_data["Exchange"].isin(exchange_list)
        & csv_data["Ticker"].str.isalpha()
    ]


def create_investable_universe(
    api_key,
    extra_tickers=None,
):
    """
    Creates an investable universe of tickers based on specified indices and extra tickers.
    By default, the investable universe is created based on the Russell 3000 and S&P 500 indices
    and only includes tickers supported by the Finnhub API. Extra tickers can be included in the
    investable universe by providing a list to the extra_tickers parameter.

    :param str api_key: API key for accessing the Finnhub API.
    :param extra_tickers: A list of extra tickers to include in the investable universe that
    are not contained in the Russell 3000 or S&P 500.
    :type extra_tickers: list or None
    :return: A DataFrame containing the supported universe of tickers and dummy variables for
    the indices they are contained in, if any.
    :rtype: pandas.DataFrame
    """

    # Load Supported Tickers
    logger.info("Fetching supported tickers from Finnhub...")
    symbols_url = f"https://finnhub.io/api/v1/stock/symbol?exchange=US&token={api_key}"
    symbols_response = requests.get(symbols_url)
    symbols_df = pd.DataFrame(symbols_response.json())
    logger.info(f"Fetched {len(symbols_df)} supported tickers from Finnhub")

    # List of exchanges to keep
    exchange_list = [
        "NASDAQ",
        "New York Stock Exchange Inc.",
        "Nyse Mkt Llc",
        "Cboe BZX formerly known as BATS",
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
    logger.info("Fetching index data from iShares...")
    for index_name, index_url in index_info_list:
        # Get data
        index_data = get_index_data(index_url, exchange_list)
        index_data["index_name"] = index_name
        index_data["date"] = pd.Timestamp.now().date()
        index_data["datetime"] = pd.Timestamp.now()

        # Append to the list
        all_indices_data.append(index_data)
    logger.info(f"Fetched index data for {len(all_indices_data)} indices")

    # Concatenate all DataFrames
    all_indices_df = pd.concat(all_indices_data, ignore_index=True)

    # Add Extra Tickers to be included in the investable universe, if any
    if extra_tickers:
        logger.info(
            f"Adding extra tickers to investable universe: {', '.join(extra_tickers)}"
        )
        extra_tickers_df = pd.DataFrame(extra_tickers, columns=["Ticker"])

        # Get the unique indices and create column names with 'in_' prefix
        unique_indices = all_indices_df["index_name"].unique()
        index_columns = ["in_" + index_name for index_name in unique_indices]

        # Initialize the indices columns with 0 for extra tickers
        for column in index_columns:
            extra_tickers_df[column] = 0

        # Append the extra tickers to all_indices_df
        combined_df = pd.concat([all_indices_df, extra_tickers_df], ignore_index=True)

    else:
        combined_df = all_indices_df

    # Convert index_name into dummy variables
    logger.info("Creating dummy variables for indices...")
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
    logger.info(
        f"Created investable universe with {len(supported_universe_df)} tickers"
    )

    # Ensure the finnhub/datasets directory exists
    if not os.path.exists("finnhub/datasets"):
        os.makedirs("finnhub/datasets")

    # Save the Investable Universe to CSV
    supported_universe_df.to_csv(
        os.path.join("finnhub/datasets/finnhub_investable_universe.csv"), index=False
    )
    logger.info(
        f"Saved investable universe to finnhub/datasets/finnhub_investable_universe.csv"
    )

    return supported_universe_df


def get_endpoint_config(
    endpoint,
    sub_endpoint,
    **kwargs,
):
    """
    Load API settings and parameters for a given endpoint and/or sub_endpoint.

    :param str endpoint: The name of the endpoint.
    :param sub_endpoint: The name of the sub_endpoint, defaults to None.
    :type sub_endpoint: str or None
    :param kwargs: Additional parameters to override the config parameters.
    :type kwargs: dict or None
    :return: A dictionary with "api" containing the API settings, "params" containing the
    parameters, and "data_keys" containing the data keys.
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


def get_endpoint_url_function(
    endpoint,
    params,
):
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
    api_key,
    ticker,
    endpoint_url_function,
    semaphore,
    api_settings,
    data_keys,
    endpoint,
):
    """
    Asynchronously fetches Finnhub data for a given ticker.

    :param str api_key: API key for accessing the Finnhub API.
    :param str ticker: Stock ticker symbol.
    :param function endpoint_url_function: Function to generate endpoint URL.
    :param asyncio.Semaphore semaphore: Semaphore to control concurrent requests.
    :param dict api_settings: Configuration dictionary containing 'api_delay' and 'query_max' values.
    :param dict data_keys: Configuration dictionary containing the data keys for the endpoint.
    :param str endpoint: The name of the Finnhub endpoint.
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
                            if data_json_key is None or pd.isna(data_json_key):
                                # If the json is not nested, create a DataFrame from the data
                                if endpoint == "profile" or endpoint == "price-target":
                                    output = pd.DataFrame([data])
                                # If the json is nested, create a DataFrame from the nested data
                                else:
                                    output = pd.DataFrame(data)
                            else:
                                output = pd.DataFrame(data[data_json_key])
                            output["ticker"] = ticker
                            logger.info(
                                f"Fetched {len(output)} rows of data for {ticker} from {full_url}"
                            )
                            return output
                        else:
                            logger.warning(
                                f"No {endpoint} data returned for {ticker}. Skipping..."
                            )
                            return None
            except aiohttp.ClientError as e:
                logger.error(f"Error fetching {endpoint} data for {ticker}: {e}")

        return


async def fetch_data_for_tickers(
    api_key,
    tickers,
    endpoint_url_function,
    api_settings,
    data_keys,
    endpoint,
):
    """
    Asynchronously fetches Finnhub data for a list of tickers.

    :param str api_key: API key for accessing the Finnhub API.
    :param list tickers: List of stock ticker symbols.
    :param function endpoint_url_function: Function to generate endpoint URLs.
    :param dict api_settings: Configuration dictionary containing 'simultaneous_connections', 'api_delay', and 'query_max' values.
    :param dict data_keys: Configuration dictionary containing the data keys for the endpoint.
    :param str endpoint: The name of the Finnhub endpoint.
    :return: The endpoint data for the given tickers.
    :rtype: pd.DataFrame
    """

    # Initialize semaphore for concurrent requests
    semaphore = asyncio.Semaphore(int(api_settings.get("simultaneous_connections")))

    # Fetch data for each ticker and gather successes and failures
    results = await asyncio.gather(
        *(
            fetch_data_for_ticker(
                api_key,
                ticker,
                endpoint_url_function,
                semaphore,
                api_settings,
                data_keys,
                endpoint,
            )
            for ticker in tickers
        )
    )
    return pd.concat(results, ignore_index=True)


def fetch_data_for_endpoint(
    api_key,
    endpoint,
    sub_endpoint=None,
    tickers=None,
    **kwargs,
):
    """
    Fetches data from a specified Finnhub endpoint for all tickers in the investable universe or provided tickers.

    :param str api_key: API key for accessing the Finnhub API.
    :param str endpoint: The name of the Finnhub endpoint.
    :param str sub_endpoint: (optional) The name of the Finnhub sub-endpoint such as "bs_annual" within the "financials" endpoint.
    :param list tickers: (optional) List of stock ticker symbols to fetch data for.
    :param dict kwargs: Additional params to override default or pass to the endpoint. For example, dynamic "start_date" and "end_date" for the "candle" endpoint.
    :return: None
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

    # Create and load the Finnhub investable universe if tickers are not provided
    if tickers is None:
        logger.info(f"Fetching your investable universe...")
        investable_universe_path = os.path.join(
            "finnhub/datasets/finnhub_investable_universe.csv"
        )
        # Check if the investable universe file exists and is up to date
        logger.info(
            f"Checking if {investable_universe_path} exists and is up to date..."
        )
        if os.path.exists(investable_universe_path) and (
            datetime.datetime.now().date()
            == datetime.datetime.fromtimestamp(
                os.path.getmtime(investable_universe_path)
            ).date()
        ):
            finnhub_universe_df = pd.read_csv(investable_universe_path)
        else:
            logger.info(
                f"Creating and saving your investable universe to {investable_universe_path}..."
            )
            finnhub_universe_df = create_investable_universe(
                api_key=api_key,
                extra_tickers=["HBB", "MMYT", "TGLS", "CYRX", "PROF", "EVLV"],
            )
        tickers = finnhub_universe_df["Ticker"].tolist()
        logger.info(f"Shuffling {len(tickers)} tickers from your investable universe")
        random.shuffle(tickers)  # Randomize the order of the tickers

    # Fetch from Finnhub API
    if sub_endpoint:
        logger.info(
            f"Fetching {len(tickers)} tickers from {endpoint}/{sub_endpoint}..."
        )
    else:
        logger.info(f"Fetching data for {len(tickers)} tickers from {endpoint}...")

    # Fetch data for the tickers
    data = asyncio.run(
        fetch_data_for_tickers(
            api_key=api_key,
            tickers=tickers,
            endpoint_url_function=endpoint_url_function,
            api_settings=config.get("api"),
            data_keys=config.get("data_keys"),
            endpoint=endpoint,
        )
    )
    if sub_endpoint:
        logger.info(f"Fetched {len(data)} rows of data from {endpoint}/{sub_endpoint}")
    else:
        logger.info(f"Fetched {len(data)} rows of data from {endpoint}")

    # Ensure the finnhub/datasets directory exists
    if not os.path.exists("finnhub/datasets"):
        os.makedirs("finnhub/datasets")

    # Store data to finnhub folder
    data.to_csv(f"finnhub/datasets/{data_file_name}.csv", index=False)
    logger.info(f"Stored data from {endpoint} to finnhub/datasets/{data_file_name}.csv")

    return
