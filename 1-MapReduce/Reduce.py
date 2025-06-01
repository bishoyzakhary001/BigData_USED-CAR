#!/usr/bin/env python3

import sys
print("Reduce.py started", file=sys.stderr)
current_key = None
count = 0
min_price = float('inf')
max_price = float('-inf')
total_price = 0.0
years = set()

for line in sys.stdin:
    print(f"Line received: {line.strip()}", file=sys.stderr)
    try:
        key, value = line.strip().split('\t')
        parts = value.split(',') #dividamo la riga in chiave (marca modello)
        # e value
        c = int(parts[0]) #numero di auto
        p_min = float(parts[1]) #min
        p_max = float(parts[2]) #max
        p_sum = float(parts[3]) #sum
        year = parts[4] # anno viecolo 

        if key != current_key and current_key is not None:
            make, model = current_key.split(':')
            avg_price = total_price / count if count else 0
            print(f"{make}\t{model}\t{count}\t{min_price:.2f}\t{max_price:.2f}\t{avg_price:.2f}\t{sorted(years)}")
            # Reset
            count = 0
            min_price = float('inf')
            max_price = float('-inf')
            total_price = 0.0
            years = set()

        current_key = key
        count += c
        min_price = min(min_price, p_min)
        max_price = max(max_price, p_max)
        total_price += p_sum
        years.add(int(year))

    except:
        continue

# Ultima chiave
if current_key:
    make, model = current_key.split(':')
    avg_price = total_price / count if count else 0
    print(f"{make}\t{model}\t{count}\t{min_price:.2f}\t{max_price:.2f}\t{avg_price:.2f}\t{sorted(years)}")