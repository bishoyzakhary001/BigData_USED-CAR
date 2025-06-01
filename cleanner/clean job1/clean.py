import pandas as pd
import re


with open('/content/drive/MyDrive/us_used_cars_clean_job1.csv', 'r', encoding='utf-8') as f:
    raw_lines = f.readlines()

cleaned_data = []

for i, line in enumerate(raw_lines):
    # Rimuovi tabulazioni e spazi multipli
    line = re.sub(r'\s+', ' ', line.strip())

    if i == 0:
        continue  # Salta header

    # Prova a usare le virgole come separatore principale
    if ',' in line:
        parts = [p.strip() for p in line.split(',')]
    else:
        parts = line.split()

    try:
        if len(parts) >= 4:
            # Assume le ultime due colonne sono prezzo e anno
            price = float(parts[-2])
            year = int(parts[-1])
            make = parts[0]
            model = ' '.join(parts[1:-2])
        else:
            print(f"Riga scartata (lunghezza {len(parts)}):", parts)
            continue

        cleaned_data.append([make, model, price, year])
    except Exception as e:
        print(f"Errore nella riga {i}: {parts} -> {e}")
        continue


df = pd.DataFrame(cleaned_data, columns=['make', 'model_name', 'price', 'year'])

# Salva in CSV con separatore chiaro
df.to_csv("/content/drive/MyDrive/us_used_cars_cleaned_job1.csv", index=False, sep=';')

print(df.head())