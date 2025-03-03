#!/usr/bin/env python3
import sys

current_coin = None
sum_price = 0.0
sum_price_sq = 0.0
sum_volume = 0.0
min_price = float('inf')
max_price = float('-inf')
total_count = 0

def emit_result(coin_id):
    if total_count == 0:
        return
    avg_price = sum_price / total_count
    variance = (sum_price_sq / total_count) - (avg_price ** 2)
    std_dev = variance ** 0.5 if variance > 0 else 0.0
    print(f"{coin_id}\t{min_price},{max_price},{avg_price},{std_dev},{sum_volume},{total_count}")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        key, values = line.split("\t", 1)
        price_str, price_sq_str, vol_str, c_str = values.split(",")
        price_val = float(price_str)
        price_sq_val = float(price_sq_str)
        vol_val = float(vol_str)
        c_val = int(c_str)
    except (ValueError, IndexError):
        continue

    if current_coin and key != current_coin:
        emit_result(current_coin)
        sum_price = 0.0
        sum_price_sq = 0.0
        sum_volume = 0.0
        min_price = float('inf')
        max_price = float('-inf')
        total_count = 0

    current_coin = key
    sum_price += price_val
    sum_price_sq += price_sq_val
    sum_volume += vol_val
    min_price = min(min_price, price_val)
    max_price = max(max_price, price_val)
    total_count += c_val

if current_coin:
    emit_result(current_coin)