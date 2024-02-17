# Finnhub Data Fetcher

The Finnhub Data Fetcher is a Python module designed for asynchronously fetching data from various endpoints of the Finnhub API. It supports fetching data for a list of stock tickers across different endpoints such as company profiles, financials, market data, and estimates. The module is capable of handling API usage parameters, reading settings from configuration files, and storing fetched data efficiently.

## Features

- Asynchronous data fetching from the Finnhub API
- Supports multiple endpoints including company profiles, financials, and market data
- Configurable API usage parameters (e.g., number of simultaneous connections, API call delay)
- Reads API settings and endpoint parameters from configuration files
- Saves fetched data to specified directories

## Key Components

- `fetch_finnhub.py`: Main module for fetching data from the Finnhub API. It includes functions for fetching data for single or multiple tickers, handling API settings, and storing fetched data.

- `fundamentals.py` & `market.py`: Examples of how to use the `fetch_finnhub.py` module to fetch data for specific endpoints.

- `.gitignore`: Configuration file to exclude certain directories and files (e.g., datasets, logs) from version control.

## Usage

### Fetching Data for Endpoints

To fetch data for a specific endpoint, use the `fetch_data_for_endpoint` function:

```python
fetch_data_for_endpoint(endpoint="profile")
```

To fetch data from an endpoint that has multiple "sub-endpoints" (e.g., financials), use the `fetch_data_for_endpoint` function with the `sub_endpoint` parameter:

```python
fetch_data_for_endpoint(endpoint="financials", sub_endpoint="bs_annual", tickers=["AAPL", "MSFT", "GOOGL"])
```

## Configuration

API settings, endpoint parameters, and data keys are configured through CSV files stored in `finnhub/configs` folder. These configurations are loaded at runtime to customize the behavior of the data fetching process.

## Logging

The module logs its operations, including info and error messages, to both a file (`finnhub.log`) and the console. This helps in monitoring the fetching process and troubleshooting issues.

## Data Storage

Fetched data is stored in the `finnhub/datasets` directory created on your machine. The `.gitignore` file is configured to exclude this directory from version control to avoid uploading large data files to the repository.

## Dependencies

- Python 3.6+

- [aiohttp](https://docs.aiohttp.org/en/stable/): Asynchronous HTTP client/server framework

- [pandas](https://pandas.pydata.org/): Data manipulation and analysis library

## Installation

Ensure you have Python 3.6+ installed. Clone the repository and install the required dependencies:

```bash
pip install aiohttp pandas
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
