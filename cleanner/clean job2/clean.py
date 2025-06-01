import pandas as pd
import string
from collections import Counter
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords

stop_words = set(stopwords.words('english'))

def clean_text(text):
    # Rimuovi "additional info" e simili
    text = text.lower()
    if "[!@@additional info@@!]" in text:
        text = text.replace("[!@@additional info@@!]", "")
    # Rimuovi punteggiatura ma lascia numeri e lettere con punto ,sostituisco punteggiatura standard tranne il punto
    punct_to_remove = string.punctuation.replace('.', '')  # togli il punto da punteggiatura da rimuovere
    text = text.translate(str.maketrans('', '', punct_to_remove))
    return text

def extract_top_words(row, feature_cols, description_col):
    # Se c'è la colonna description, usala, altrimenti unisci le colonne feature_x
    if description_col in row and pd.notnull(row[description_col]):
        text = str(row[description_col])
    else:
        text = ' '.join([str(row[col]) for col in feature_cols if pd.notnull(row[col])])
    
    text = clean_text(text)
    tokens = text.split()
    # Rimuovi stopwords
    tokens = [word for word in tokens if word not in stop_words]
    
    freq = Counter(tokens)
    top3 = [word for word, count in freq.most_common(3)]
    return top3

input_path = '/content/drive/MyDrive/us_used_cars_clean_job2.csv'
output_path = '/content/drive/MyDrive/us_used_cars_cleaned_job2.csv'

chunk_size = 100000
first_chunk = True

for chunk in pd.read_csv(input_path, sep=',', chunksize=chunk_size, engine='python', on_bad_lines='skip'):
    feature_cols = [col for col in chunk.columns if col.startswith('feature_')]
    description_col = 'description' if 'description' in chunk.columns else None
    
    chunk['top_words'] = chunk.apply(lambda row: extract_top_words(row, feature_cols, description_col), axis=1)
    
    output_cols = ['city', 'year', 'price', 'top_words']
    chunk_output = chunk[output_cols]
    
    if first_chunk:
        chunk_output.to_csv(output_path, sep=';', index=False)
        first_chunk = False
    else:
        chunk_output.to_csv(output_path, sep=';', index=False, mode='a', header=False)

    print(chunk_output.head())

print("Done processing all chunks.")