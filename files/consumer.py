import requests
import pandas as pd
import json
from datetime import datetime

# API endpoint
url = "https://api.coingecko.com/api/v3/coins/markets"
params = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 100,
    "page": 1,
    "sparkline": "false"
}

# Fetch data
response = requests.get(url, params=params)

if response.status_code == 200:
    data = response.json()
    
    # Convert to DataFrame
    df = pd.DataFrame(data)[["id", "symbol", "name", "current_price", "market_cap", "total_volume", "high_24h", "low_24h"]]
    
    # Add timestamp
    df["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Save as CSV
    df.to_csv("crypto_data.csv", index=False)
    print("Data saved successfully.")
else:
    print(f"Error: {response.status_code} - {response.text}")
