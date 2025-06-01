#!/usr/bin/env python3
import sys
import csv

reader = csv.reader(sys.stdin, delimiter=';')
header = next(reader)  # Salta intestazione
print(f"Header: {header}", file=sys.stderr)  # DEBUG

for row in reader:
    print(f"Riga letta: {row}", file=sys.stderr)  # DEBUG
    try:
        make = row[0].strip()
        model = row[1].strip()
        price = float(row[2])
        year = row[3].strip()
        key = f"{make}:{model}"
        # count=1, min price, max price, sum price, year
        print(f"{key}\t1,{price},{price},{price},{year}")
    except Exception as e:
        print(f"Errore su riga {row}: {e}", file=sys.stderr)
        continue