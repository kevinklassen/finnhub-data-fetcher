from modules.fetch_finnhub import fetch_data_for_endpoint
from modules.store_finnhub import write_to_jsonl

# Fetch from Finnhub API
data = fetch_data_for_endpoint(endpoint="ebit-estimate")

# Store data in JSONL file
file_path = "test/data/json/finnhub_ebit_estimate.jsonl"
write_to_jsonl(data, file_path)
