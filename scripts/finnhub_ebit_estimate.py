from modules.fetch_finnhub import fetch_finnhub_data

# Parameters
ENDPOINT = "ebit-estimate"
SIMULTANEOUS_CONNECTIONS = 2
API_DELAY = 6 / 7
QUERY_MAX = 5

# Fetch from Finnhub API
results_df, api_call_timestamps = fetch_finnhub_data(
    endpoint=ENDPOINT,
    simultaneous_connections=SIMULTANEOUS_CONNECTIONS,
    api_delay=API_DELAY,
    query_max=QUERY_MAX,
)