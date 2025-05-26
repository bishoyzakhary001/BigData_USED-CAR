import random
import pandas as pd

def modify_value(value):
    if isinstance(value, (int, float)):
        return value * random.random()
    else:
        return value

def process_csv(file_path):
   
    df = pd.read_csv(file_path)
    
    
    df_doubled = pd.concat([df, df], ignore_index=True)
    

    df_modified = df_doubled.map(modify_value)
    
    # Salva il DataFrame modificato in un nuovo file CSV
    output_file_path = file_path.replace("_pulito.csv", "_double.csv")
    df_modified.to_csv(output_file_path, index=False)
    print(f"File salvato come {output_file_path}")


process_csv("/Users/bishoyzakhary/Documents/GitHub/BigData_USED-CAR/used_cars_data.csv")
