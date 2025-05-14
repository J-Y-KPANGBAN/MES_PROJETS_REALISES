import pandas as pd
import requests


def extract_1(url):#format CSV
    try:
        data=pd.read_csv(url) # "skip" ignore les lignes mal formatées ,on_bad_lines='skip'
        return data
    except Exception as e :
        print( "statut",e)
        return None


"""def extract(url):# format json
    file=requests.get(url)
    if file.ok:
        data =file.json()
        df= pd.dataframe(data)
        return df
    else:
        print("reponse sur le fichier",file.status_code)
        return None
    return df
"""
#format bd

def tranform(df,TVA):
# Ajouter une nouvelle colonne 'Montant' en multipliant Quantité et PrixUnitaire(€)
    df['Montant'] = df['Quantité'] * df['Prix Unitaire (€)']# calculer le montant total par  produit
    df['TVA'] = df['Montant'] * TVA # calculer le TVA 
    return df


def load(data, path):
    try:
        # Sauvegarde des données dans un fichier Excel
        data.to_excel(path, index=False, engine='openpyxl') #df.to_csv("foo.csv")
        print("Succès ! Les données ont été chargées dans le fichier Excel.")
    except Exception as e:
        print("Échec lors du chargement des données dans le fichier Excel. Erreur :",e)

#exécution
def etl_data():
    # les variables url du fichier à tranformer et path pour le lieu ou mon file sera charger
    url ="https://raw.githubusercontent.com/J-Y-KPANGBAN/PROJET_etl_v1/refs/heads/main/stock_grande_surface.csv"
    path ="C:/Users/Jean-YvesDG/Downloads/projet_gestion_ETL_python/vente_stockage/mon_file.xlsx"
    taux_TVA= 0.25 # le taux de tva = 25%
    # appel de la fonction extract_1()
    df =extract_1(url) 
    #appel de la fonction tranform()
    df_transf=tranform(df,taux_TVA) 
    #chargement d'un nouveau fichier  suit la la transformation 
    load(df,path) 
    

etl_data()
