import kagglehub
import pandas as pd
import os


path = kagglehub.dataset_download("ananaymital/us-used-cars-dataset")

print("Path to dataset files:", path)


base_path = path


dataset_path = None  
for file in os.listdir(base_path):
    if file.endswith(".csv"):
        dataset_path = os.path.join(base_path, file)
        break

# Carica il file
if dataset_path:
    df = pd.read_csv(dataset_path, low_memory=False)
    print(f"Dataset caricato da: {dataset_path}")
    print(df.head())
else:
    print(f"No CSV file found in the directory: {base_path}")
df_job2 = df[['city', 'year', 'price', 'daysonmarket', 'description']].copy()
df_job2 = df_job2.dropna()
df_job2 = df_job2[(df_job2['price'] > 0) & (df_job2['year'] >= 1980) & (df_job2['year'] <= 2025)]
df_job2 = df_job2[df_job2['daysonmarket'] >= 0]
df_job2['city'] = df_job2['city'].str.strip().str.lower()
df_job2['description'] = df_job2['description'].str.lower()

# Salva
df_job2.to_csv("/content/drive/MyDrive/us_used_cars_clean_job2.csv",index=False)

