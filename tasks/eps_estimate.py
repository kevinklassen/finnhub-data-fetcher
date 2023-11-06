from datetime import datetime
from modules.fetch_finnhub import fetch_data_for_endpoint
from modules.store_finnhub import write_to_jsonl

# Fetch from Finnhub API
data = fetch_data_for_endpoint(endpoint="eps-estimate")

# Generate the filename based on the current date
current_date = datetime.now().strftime("%Y_%m_%d")
file_path = f"finnhub_eps_estimate/{current_date}.jsonl"

# Store data in JSONL file
write_to_jsonl(data, file_path)
