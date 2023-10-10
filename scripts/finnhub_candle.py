from datetime import datetime, date, timedelta
from modules.fetch_finnhub import fetch_finnhub_data

# Parameters
ENDPOINT = "candle"
SIMULTANEOUS_CONNECTIONS = 1
API_DELAY = 1
QUERY_MAX = 5
RESOLUTION = "D"
START_DATE = int(
    datetime.strptime(str(date.today() - timedelta(days=7)), "%Y-%m-%d").timestamp()
)
END_DATE = int(datetime.strptime(str(date.today()), "%Y-%m-%d").timestamp())

# Fetch from Finnhub API
results_df, api_call_timestamps = fetch_finnhub_data(
    endpoint=ENDPOINT,
    simultaneous_connections=SIMULTANEOUS_CONNECTIONS,
    api_delay=API_DELAY,
    query_max=QUERY_MAX,
    resolution=RESOLUTION,
    start_date=START_DATE,
    end_date=END_DATE,
)
