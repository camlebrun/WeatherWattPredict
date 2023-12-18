import os
import requests
import pandas as pd
from io import BytesIO
import gzip

# Définir le répertoire de destination
destination_directory = "./data/"  # Remplacez cela par le chemin de votre choix

# Vérifier si le répertoire de destination existe, sinon le créer
if not os.path.exists(destination_directory):
    os.makedirs(destination_directory)

url_template = "https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/BASE/QUOT/Q_{departement}_previous-1950-2021_RR-T-Vent.csv.gz"

# Créer une liste vide pour stocker les DataFrames
merged_df = pd.DataFrame()

for departement_number in range(1, 99):
    # Ignorer le département 62
    if departement_number != 62:
        # Formater l'URL avec le numéro du département
        url = url_template.format(departement=str(departement_number).zfill(2))

        # Réaliser la requête HTTP pour télécharger le fichier
        response = requests.get(url, stream=True)

        # Vérifier si la requête a réussi (code 200)
        if response.status_code == 200:
            # Utiliser BytesIO pour traiter les données en mémoire
            with BytesIO(response.content) as compressed_file:
                # Utiliser gzip pour décompresser les données
                with gzip.open(compressed_file, 'rt') as csv_file:
                    try:
                        print(f"Lecture du fichier CSV pour le département {departement_number}")
                        # Lire le CSV dans un DataFrame pandas
                        df = pd.read_csv(csv_file)

                        # Fusionner le DataFrame au DataFrame existant
                        merged_df = pd.concat([merged_df, df], ignore_index=True)

                        # Print un message quand le fichier CSV est fusionné
                        print(f"Le fichier CSV a été fusionné : {departement_number}")

                    except pd.errors.ParserError as e:
                        # Print un message en cas d'erreur de lecture du CSV
                        print(f"Erreur lors de la lecture du fichier CSV pour le département {departement_number}: {e}")
        else:
            # Print un message en cas d'échec de téléchargement
            print(f"Échec du téléchargement pour le département {departement_number}. Code de statut HTTP : {response.status_code}")

# Définir le chemin du fichier de sortie dans le répertoire de destination
output_file_path = os.path.join(destination_directory, "data_departements.csv")

# Écrire les données fusionnées dans le fichier CSV de sortie
merged_df.to_csv(output_file_path, index=False)

print(f"Le fichier CSV fusionné a été créé : {output_file_path}")
