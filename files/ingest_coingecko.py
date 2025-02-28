#!/usr/bin/env python3
import requests
import json
import subprocess
from datetime import datetime

def fetch_coingecko_data():
    """Fetch top 100 cryptocurrencies from CoinGecko API."""
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': 100,
        'page': 1,
        'sparkline': 'false'
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"API request failed with status {response.status_code}")
    return response.json()

def store_in_hdfs(data, execution_date):
    """Store raw JSON data in HDFS with date partitioning."""
    # Convert data to JSON string
    json_data = json.dumps(data)
    
    # Local temp file
    local_file = "/tmp/coingecko_raw.json"
    with open(local_file, 'w') as f:
        f.write(json_data)
    
    # Partition path based on date (e.g., YYYY=2025/MM=01/DD=01)
    year = execution_date.strftime("%Y")
    month = execution_date.strftime("%m")
    day = execution_date.strftime("%d")
    hdfs_dir = f"/user/etudiant/crypto/raw/YYYY={year}/MM={month}/DD={day}"
    hdfs_file = f"{hdfs_dir}/coingecko_raw.json"
    
    # Create directory and upload file
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=True)
    subprocess.run(["hdfs", "dfs", "-put", "-f", local_file, hdfs_file], check=True)
    print(f"Stored data in {hdfs_file}")

if __name__ == "__main__":
    # Use today’s date for testing
    today = datetime.now()
    data = fetch_coingecko_data()
    store_in_hdfs(data, today)