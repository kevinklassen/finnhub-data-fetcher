from datetime import datetime, date, timedelta
from modules.fetch_finnhub import fetch_data_for_endpoint
from modules.store_finnhub import write_to_jsonl

# Compute dynamic params for candle endpoint
start_date = int(
    datetime.strptime(str(date.today() - timedelta(days=8)), "%Y-%m-%d").timestamp()
)
end_date = int(
    datetime.strptime(str(date.today() - timedelta(days=1)), "%Y-%m-%d").timestamp()
)

# Fetch from Finnhub API
data = fetch_data_for_endpoint(
    endpoint="candle", start_date=start_date, end_date=end_date
)

# Store data in JSONL file
file_path = "test/data/json/finnhub_candle.jsonl"
write_to_jsonl(data, file_path)
