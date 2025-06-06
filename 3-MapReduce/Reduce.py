#!/usr/bin/env python3
import sys

current_key = None
group = []

def emit_group(key, group):
    if not group:
        return
    total_price = sum(x[1] for x in group)
    avg_price = total_price / len(group)
    top_model = max(group, key=lambda x: x[2])[0]
    print(f"{key}\t{avg_price:.2f}\t{top_model}")

for line in sys.stdin:
    print(f"Reduce received line: {line}", file=sys.stderr)
    line = line.strip()
    if not line:
        continue
    try:
        key, value = line.strip().split('\t')
        model, price, hp, disp = value.split(',')
        price, hp, disp = float(price), float(hp), float(disp)

        if key != current_key and current_key is not None:
            emit_group(current_key, group)
            group = []

        current_key = key
        group.append((model, price, hp, disp))
    except:
        continue

# Ultimo gruppo
emit_group(current_key, group)