import boto3
from smart_open import open as s3_open
import csv
import re
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

# --- CONFIGURATION ---
BUCKET_NAME = "meditatio-adjust"
OUTPUT_FILE = "raw_aws_data.csv"
TIMESTAMP_THRESHOLD = "2022-01-01T000000"

# Vos colonnes spécifiques
COLUMNS_TO_KEEP = [
    '{reporting_revenue}', '{event_name}', '{created_at}', 
    '{installed_at}', '{random_user_id}', '{subscription_event_type}', 
    '{currency}', '{campaign_name}', '{tracker}', '{adgroup_name}'
]

# On définit les clés pour le filtrage (avec accolades comme dans les fichiers)
COL_REVENUE = '{reporting_revenue}'
COL_EVENT = '{event_name}'

# -----------------------------------------------------------------------------

def is_valid(row):
    """Vérifie si la ligne contient les données essentielles."""
    rev = row.get(COL_REVENUE)
    evt = row.get(COL_EVENT)
    return bool(rev and rev.strip()) and bool(evt and evt.strip())

def process_single_file(s3_key):
    """Lit un fichier S3 et renvoie les lignes filtrées."""
    rows = []
    s3_uri = f"s3://{BUCKET_NAME}/{s3_key}"
    try:
        with s3_open(s3_uri, 'r', encoding='utf-8') as fin:
            reader = csv.DictReader(fin)
            for row in reader:
                if is_valid(row):
                    # Extraction et nettoyage des colonnes demandées
                    filtered_row = {col: row.get(col, "").strip() for col in COLUMNS_TO_KEEP}
                    rows.append(filtered_row)
    except Exception as e:
        tqdm.write(f"Erreur sur {s3_key}: {e}")
    return rows

def get_filtered_files_list(s3_client):
    """Récupère la liste des clés S3 filtrées par date."""
    print("Analyse du bucket S3...")
    paginator = s3_client.get_paginator('list_objects_v2')
    pattern = re.compile(r'(\d{4}-\d{2}-\d{2}T\d{6})')
    valid_keys = []
    
    for page in paginator.paginate(Bucket=BUCKET_NAME):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.endswith('.csv.gz'):
                continue
            match = pattern.search(key)
            if match and match.group(1) >= TIMESTAMP_THRESHOLD:
                valid_keys.append(key)
    return valid_keys

def generate_csv():
    s3_client = boto3.client('s3')
    keys_to_process = get_filtered_files_list(s3_client)
    
    if not keys_to_process:
        print("Aucun fichier à traiter.")
        return

    print(f"Fusion de {len(keys_to_process)} fichiers via ThreadPool...")

    with open(OUTPUT_FILE, 'w', encoding='utf-8', newline='') as fout:
        writer = csv.DictWriter(fout, fieldnames=COLUMNS_TO_KEEP)
        writer.writeheader()

        # Utilisation de ThreadPoolExecutor pour paralléliser les téléchargements/lectures
        # max_workers=10 est un bon compromis pour ne pas saturer la bande passante
        with ThreadPoolExecutor(max_workers=10) as executor:
            # map conserve l'ordre (optionnel ici mais propre)
            results = list(tqdm(executor.map(process_single_file, keys_to_process), 
                                total=len(keys_to_process), 
                                desc="Traitement", 
                                unit="file"))

        # Écriture des résultats consolidés
        print("Écriture du fichier final...")
        total_rows = 0
        for file_rows in results:
            if file_rows:
                writer.writerows(file_rows)
                total_rows += len(file_rows)

    print(f"\nTerminé !")
    print(f"- Fichier : {OUTPUT_FILE}")
    print(f"- Total lignes : {total_rows:,}")

if __name__ == "__main__":
    generate_csv()