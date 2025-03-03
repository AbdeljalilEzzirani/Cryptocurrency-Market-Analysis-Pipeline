#!/usr/bin/env python3
import sys
import json

# Read the entire JSON file from stdin
input_data = sys.stdin.read().strip()
if not input_data:
    sys.exit(0)

try:
    records = json.loads(input_data)  # Parse the JSON array
except json.JSONDecodeError:
    sys.exit(0)

for record in records:
    coin_id = record.get('id')
    current_price = record.get('current_price')
    volume = record.get('total_volume')

    # Basic validation
    if not coin_id or current_price is None or volume is None:
        continue

    try:
        price_float = float(current_price)
        volume_float = float(volume)
    except (ValueError, TypeError):
        continue

    price_squared = price_float * price_float
    # Emit: coin_id \t price,price_squared,volume,count
    print(f"{coin_id}\t{price_float},{price_squared},{volume_float},1")