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

if dataset_path:
    df = pd.read_csv(dataset_path, low_memory=False)
    print(f"Dataset caricato da: {dataset_path}")
    print(df.head())
else:
    print(f"No CSV file found in the directory: {base_path}")
df_job3 = df[['model_name', 'price', 'horsepower', 'engine_displacement']].copy()
df_job3 = df_job3.dropna()
df_job3 = df_job3[(df_job3['price'] > 0) & (df_job3['horsepower'] > 0) & (df_job3['engine_displacement'] > 0)]
df_job3['model_name'] = df_job3['model_name'].str.strip().str.lower()

# Salva
df_job3.to_csv("/content/drive/MyDrive/us_used_cars_clean_job3.csv", index=False)
