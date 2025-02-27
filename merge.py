import pandas as pd
import os

# Verzeichnis mit den Excel-Dateien
source_dir = "sources"
output_file = "merged_profiles.xlsx"

def merge_and_deduplicate(directory, output_file):
    all_dfs = []
    
    # Alle Excel-Dateien im Verzeichnis einlesen
    for file in os.listdir(directory):
        if file.endswith(".xlsx") or file.endswith(".xls"):
            file_path = os.path.join(directory, file)
            df = pd.read_excel(file_path)
            all_dfs.append(df)
    
    # Datensätze zusammenführen
    merged_df = pd.concat(all_dfs, ignore_index=True)
    
    # Doppelte Einträge entfernen
    deduplicated_df = merged_df.drop_duplicates()
    
    # Bereinigte Datei speichern
    deduplicated_df.to_excel(output_file, index=False)
    
    print(f"Zusammengeführte Datei gespeichert unter: {output_file}")

# Funktion ausführen
merge_and_deduplicate(source_dir, output_file)
