from modules.fetch_finnhub import fetch_data_for_endpoint
from modules.store_finnhub import write_to_jsonl

# Fetch from Finnhub API
data = fetch_data_for_endpoint(endpoint="financials", sub_config_key="ic_annual")

# Store data in JSONL file
file_path = "test/data/json/finnhub_financials_ic_a.jsonl"
write_to_jsonl(data, file_path)
