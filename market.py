from datetime import datetime, date, timedelta
from fetch_finnhub import fetch_data_for_endpoint

# Compute dynamic params for candle endpoint
start_date = int(
    datetime.strptime(str(date.today() - timedelta(days=8)), "%Y-%m-%d").timestamp()
)
end_date = int(
    datetime.strptime(str(date.today() - timedelta(days=1)), "%Y-%m-%d").timestamp()
)

# Fetch from Finnhub API
fetch_data_for_endpoint(
    api_key="Your Finnhub API key here",
    endpoint="candle",
    start_date=start_date,
    end_date=end_date,
    resolution="D",
)
