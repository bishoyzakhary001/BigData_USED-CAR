#!/usr/bin/env python3
import sys
import csv

reader = csv.reader(sys.stdin)  # CSV standard con virgola

header = next(reader)  # salta intestazione
# print(f"Header: {header}", file=sys.stderr)  # DEBUG opzionale

for row in reader:
    print(f"Riga letta: {row}", file=sys.stderr)  # DEBUG
    try:
        if len(row) != 4:
            continue  # riga incompleta

        model = row[0]
        hp = float(row[1])
        disp = float(row[2])
        price = float(row[3])

        hp_bin = int(hp / 10)
        disp_bin = int(disp / 100)

        key = f"{hp_bin}-{disp_bin}"
        value = f"{model},{price},{hp},{disp}"
        print(f"{key}\t{value}")
    except Exception as e:
        print(f"Errore: {row} -> {e}", file=sys.stderr)
        continue