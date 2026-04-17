import requests
import json
from datetime import datetime
from pathlib import Path

url = "https://api.coingecko.com/api/v3/coins/markets"

params = {
    "vs_currency": "usd",
    "ids": "bitcoin,ethereum"
}

response = requests.get(url, params=params)

print("Status code:", response.status_code)

data = response.json()

print("Top-level type:", type(data))
print("Number of records:", len(data))

raw_dir = Path("data/raw")
raw_dir.mkdir(parents=True, exist_ok=True)

timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
output_file = raw_dir / f"market_{timestamp_str}.json"

with open(output_file, "w") as f:
    json.dump(data, f, indent=2)

print(f"Saved raw JSON to: {output_file}")
