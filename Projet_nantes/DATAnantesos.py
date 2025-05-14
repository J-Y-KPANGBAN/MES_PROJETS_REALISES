import os
import pandas as pd
import requests as rq

def extraict(url):
    reponse = rq.get(url)
    if reponse.ok:
        data = reponse.json()
        df = pd.DataFrame(data['results'])
    else:
        print(f"Erreur {reponse.status_code}")
        df = pd.DataFrame()  # Eviter erreur si échec
    return df

def transform(df):
    return {
        'parking': df[['idobj', 'nom_complet', 'libtype', 'conditions_d_acces']].copy(),
        'payement': df[['idobj', 'moyen_paiement']].copy(),
        'services': df[['idobj', 'services', 'autres_service_mob_prox']].copy(),
        'lieu': df[['idobj', 'code_postal', 'adresse', 'commune', 'quartier']].copy(),
        'geo': df[['idobj', 'long_wgs84', 'lat_wgs84']].copy(),
        'description': df[['idobj', 'presentation']].copy(),
        'capacite': df[['idobj', 'capacite_voiture', 'capacite_pmr',
                        'capacite_vehicule_electrique', 'capacite_moto', 'capacite_velo']].copy(),
        'transport': df[['idobj', 'service_velo', 'stationnement_velo', 'stationnement_velo_securise']].copy(),
        'exploitant': df[['idobj', 'exploitant']].copy(),
        'acces': df[['idobj', 'acces_transport_commun', 'nom_usuel']].copy()
    }

# Dossier d'export
dossier_export = "D_warehouse"
if not os.path.exists(dossier_export):
    os.makedirs(dossier_export)

# Exécution
url = "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_parcs-relais-nantes-metropole/records?limit=100"
df = extraict(url)
tables = transform(df)

# Export de chaque table dans le dossier D_warehouse
for name, dataframe in tables.items():
    chemin_fichier = os.path.join(dossier_export, f"{name}.csv")
    dataframe.to_csv(chemin_fichier, index=False)
