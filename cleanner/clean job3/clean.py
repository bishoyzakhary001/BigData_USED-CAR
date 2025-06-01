import pandas as pd


input_path = '/content/drive/MyDrive/us_used_cars_clean_job3.csv'
output_path = '/content/drive/MyDrive/us_used_cars_cleaned_job3.csv'

#Gestione File
df = pd.read_csv(input_path)
df['engine_displacement'] = df['engine_displacement'] / 1000
df['horsepower'] = df['horsepower'].astype(int)
df['price'] = df['price'].astype(int)
df['engine_displacement'] = df['engine_displacement'].round(1)
df = df[['model_name', 'horsepower', 'engine_displacement', 'price']]

#Salva
df.to_csv(output_path, sep=';', index=False)
print(df.head())