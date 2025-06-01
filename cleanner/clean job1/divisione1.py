import kagglehub
import pandas as pd
import os


path = kagglehub.dataset_download("ananaymital/us-used-cars-dataset")

print("Path to dataset files:", path)


base_path = path

# Trova il file CSV nel percorso
dataset_path = None  
for file in os.listdir(base_path):
    if file.endswith(".csv"):
        dataset_path = os.path.join(base_path, file)
        break


if dataset_path:
    df = pd.read_csv(dataset_path, low_memory=False)
    print(f"Dataset caricato da: {dataset_path}")
    print(df.head())
else:
    print(f"No CSV file found in the directory: {base_path}")


df_job1 = df[['make_name', 'model_name', 'price', 'year']].copy()
df_job1 = df_job1.dropna()
df_job1 = df_job1[(df_job1['price'] > 0) & (df_job1['year'] >= 1980) & (df_job1['year'] <= 2025)]
df_job1['make_name'] = df_job1['make_name'].str.strip().str.lower()
df_job1['model_name'] = df_job1['model_name'].str.strip().str.lower()


df_job1.to_csv("/content/us_used_cars_clean_job1.csv", index=False)