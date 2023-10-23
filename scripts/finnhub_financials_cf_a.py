from modules.fetch_finnhub import fetch_finnhub_data

# Parameters
ENDPOINT = "financials"
SIMULTANEOUS_CONNECTIONS = 10
API_DELAY = 2
QUERY_MAX = 5
STATEMENT = "cf"
FREQ = "annual"

# Fetch from Finnhub API
fetch_finnhub_data(
    endpoint=ENDPOINT,
    simultaneous_connections=SIMULTANEOUS_CONNECTIONS,
    api_delay=API_DELAY,
    query_max=QUERY_MAX,
    statement=STATEMENT,
    freq=FREQ,
)
