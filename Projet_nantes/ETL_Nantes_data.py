import pandas as pd
import requests as req
import time
from datetime import datetime



def p():
    pass
def extract(url):
    reponse= req.get(url)
    if reponse.ok: #Requête réussie (statut 200, 201, etc.) equivalance de ok
        data= reponse.json()
        df= pd.DataFrame(data['results'])
        return df
    else:
        print(" reponse requête",reponse.status_codes) ## Erreur (404, 500, etc.)
        return None
    return df


def load(df, path):
    try:
        df.to_excel(path,index=False,engine="openpyxl")
        print("succes")
    except Exception as e:
        print("Echec")



def etl_pipeline():
#TEST
    url="https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_parkings-publics-nantes-disponibilites/records?limit=100"
    path="C:/Users/Jean-YvesDG/Downloads/projet_gestion_ETL_python/depot_git/Nantes_data.xlsx"
    df =extract(url)
    if df is not None:
    #print("Mes données\n",df.head())
    #print("*******************\n")
    #print("les columns\n", df.columns)
        load(df,path)


# ⏱️ Exécution toutes les 2 minutes
print("🚀 Démarrage du processus ETL en boucle (toutes les 2 minutes)")
while True:
    n=+1
    try:
        etl_pipeline()
    except Exception as e:
        print("⚠️ Une erreur est survenue :", e)
    print("⏳ Attente de moins d'une minutes...\n",n)
    time.sleep(10)