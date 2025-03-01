import pandas as pd
import os

# Funktion, die die E-Mails basierend auf der Logik zuordnet
def get_email(df, email_columns, email_type_columns, target_type):
    """
    Gibt die erste E-Mail des angegebenen Typs (personal oder business) zurück.
    Überprüft zuerst die primäre E-Mail und dann die Drittanbieter-E-Mails in Reihenfolge der Präferenz.
    """
    for email_column, type_column in zip(email_columns, email_type_columns):
        if email_column in df and type_column in df:  # Überprüfen, ob die Spalten existieren
            email = df[email_column]
            email_type = df[type_column]
            
            # Überprüfen, ob eine gültige E-Mail vorhanden ist und der Typ mit dem gewünschten Zieltyp übereinstimmt
            if pd.notna(email) and email_type == target_type:
                return email, True
    return None, False  # Return None and False if no valid email of the desired type is found

# Funktion zum Mappen der Quell-Daten auf das Mautic-Format
def map_to_mautic_format(df, field_mapping):
    mautic_df = df[list(field_mapping.keys())].copy()
    # Duplicate the 'current_company' field
    mautic_df['companyname'] = mautic_df['current_company']
    mautic_df = mautic_df.rename(columns=field_mapping)
    return mautic_df

def merge_and_deduplicate(directory, output_csv, output_excel, field_mapping):
    all_dfs = []
    
    # Alle CSV-Dateien im Verzeichnis einlesen
    for file in os.listdir(directory):
        if file.endswith(".csv"):
            file_path = os.path.join(directory, file)
            try:
                # Statt error_bad_lines das Argument on_bad_lines verwenden
                df = pd.read_csv(file_path, encoding='utf-8', dtype=str, sep=None, engine='python', on_bad_lines='skip')
                all_dfs.append(df)
            except Exception as e:
                print(f"Fehler beim Einlesen von {file}: {e}")
    
    if not all_dfs:
        print("Keine gültigen CSV-Dateien gefunden!")
        return
    
    # Datensätze zusammenführen
    merged_df = pd.concat(all_dfs, ignore_index=True)
    
    # Definiere die Spalten für Emails und Email-Typen
    email_columns = ['email', 'third_party_email_1', 'third_party_email_2', 'third_party_email_3']
    email_type_columns = ['email_type', 'third_party_email_type_1', 'third_party_email_type_2', 'third_party_email_type_3']
    
    # Initialisiere die neuen Spalten für E-Mails
    merged_df['email1'] = None
    merged_df['companyemail'] = None
    
    # Fülle die 'email' und 'companyemail' Felder
    for index, row in merged_df.iterrows():
        # Finde die erste 'personal' Email für das 'email' Feld
        personal_email, found_personal = get_email(row, email_columns, email_type_columns, 'personal')
        
        # Finde die erste 'business' Email für das 'companyemail' Feld
        business_email, _ = get_email(row, email_columns, email_type_columns, 'business')
        
        # Wenn keine 'personal' Email gefunden wurde, kopiere die 'business' Email in das 'email' Feld
        if not found_personal:
            merged_df.at[index, 'email1'] = business_email
        else:
            merged_df.at[index, 'email1'] = personal_email
        
        merged_df.at[index, 'companyemail'] = business_email

    # Doppelte Einträge entfernen
    deduplicated_df = merged_df.drop_duplicates()
    
    # Mapping auf das Mautic-Format anwenden
    mautic_df = map_to_mautic_format(deduplicated_df, field_mapping)
    
    # Bereinigte Dateien speichern
    mautic_df.to_csv(output_csv, index=False, encoding='utf-8')
    mautic_df.to_excel(output_excel, index=False)
    
    # Konsolenausgabe mit Banner und wichtigsten Statistiken
    print("="*40)
    print("Programm erfolgreich ausgeführt")
    print(f"Anzahl der zusammengeführten Datensätze: {len(merged_df)}")
    print(f"Anzahl der eindeutigen Datensätze nach Duplikatentfernung: {len(deduplicated_df)}")
    print(f"Dateien gespeichert unter: {output_csv} und {output_excel}")
    print("="*40)

# Definiere das Mapping von den Original-Feldern zu den Mautic-kompatiblen Feldern
field_mapping = {
    'email1': 'email',  # E-Mail-Adresse
    'first_name': 'firstname',  # Vorname
    'last_name': 'lastname',  # Nachname
    'phone_1': 'phone',  # Telefon
    'location_name': 'address1',  # Ort
    'profile_url': 'linkedin',  # 'profile_url' wird zu 'linked_in' in Mautic
    'avatar': 'avatar',  # Avatar
    'headline': 'headline',  # Headline
    'languages': 'languages',  # Sprachen
    'skills': 'skills',  # Fähigkeiten
    'followers': 'followers',  # Follower
    'current_company': 'company',  # Unternehmen (jetzt 'current_company' -> 'company' in Mautic)
    'current_company_industry': 'companyindustry',  # Branche
    'companyemail': 'companyemail',  # Business Email für Mautic
    'current_company_position': 'position',  # 'current_company_position' wird zu 'position' in Mautic
    'organization_url_1': 'companylinkedin',  # 'organization_url_1' wird zu 'companylinkedin' in Mautic
    'organization_description_1': 'companydescription',  # 'organization_description_1' wird zu 'companydescription' in Mautic
    'organization_location_1': 'companycity',  # 'organization_location_1' wird zu 'companycity' in Mautic
    'organization_website_1': 'companywebsite',  # 'organization_website_1' wird zu 'companywebsite' in Mautic
}

# Verzeichnis mit den CSV-Dateien
source_dir = "sources"
output_csv = "merged_profiles_mautic.csv"
output_excel = "merged_profiles_mautic.xlsx"

# Funktion ausführen
merge_and_deduplicate(source_dir, output_csv, output_excel, field_mapping)
