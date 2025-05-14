import pandas as pd
import requests as rq
from sqlalchemy import create_engine # bibliotheque de connexion 

def extract(url):
    reponse = rq.get(url)
    if reponse.ok:
        data = reponse.json()
        df = pd.DataFrame(data['results'])
    else:
        print(f"Erreur {reponse.status_code}")
        df = pd.DataFrame()
    return df

# Découpage des données 
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

# Others 
"""
def transform(df):
    # Table principale : parking
    df_parking = df[['idobj', 'nom_complet', 'libtype', 'conditions_d_acces']].copy()

    # Paiement
    df_payement = df[['idobj', 'moyen_paiement']].copy()

    # Services
    df_services = df[['idobj', 'services', 'autres_service_mob_prox']].copy()

    # Localisation
    df_lieu = df[['idobj', 'code_postal', 'adresse', 'commune', 'quartier']].copy()

    # Coordonnées géographiques
    df_geo = df[['idobj', 'long_wgs84', 'lat_wgs84']].copy()

    # Description
    df_description = df[['idobj', 'presentation']].copy()

    # Capacités de stationnement
    df_capacite = df[['idobj', 'capacite_voiture', 'capacite_pmr',
                      'capacite_vehicule_electrique', 'capacite_moto', 'capacite_velo']].copy()

    # Équipements de transport doux
    df_transport = df[['idobj', 'service_velo', 'stationnement_velo',
                       'stationnement_velo_securise']].copy()

    # Exploitant
    df_exploitant = df[['idobj', 'exploitant']].copy()

    # Accès transports
    df_acces = df[['idobj', 'acces_transport_commun', 'nom_usuel']].copy()

    return {
        'parking': df_parking,
        'payement': df_payement,
        'services': df_services,
        'lieu': df_lieu,
        'geo': df_geo,
        'description': df_description,
        'capacite': df_capacite,
        'transport': df_transport,
        'exploitant': df_exploitant,
        'acces': df_acces
    }

"""

# 🔧 Paramètres de connexion à PostgreSQL
user = "votre_utilisateur"
password = "votre_mot_de_passe"
host = "localhost"  # ou une IP distante
port = "5432"
database = "votre_base"

# Chaîne de connexion SQLAlchemy
engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

# 🔁 Pipeline
url = "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_parcs-relais-nantes-metropole/records?limit=100"
df = extract(url)
tables = transform(df)

# 🧩 Injection dans PostgreSQL
for name, dataframe in tables.items():
    dataframe.to_sql(name=name, con=engine, if_exists='replace', index=False)
    print(f"✅ Table '{name}' injectée avec succès.")
