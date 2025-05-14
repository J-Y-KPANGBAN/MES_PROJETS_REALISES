import pandas as pd

# Extraction
def Extract(url):
    try:
        df = pd.read_csv(url, engine="python", on_bad_lines='warn')
        print("Lecture du fichier réussie")
    except Exception as e:
        print("Echec de la lecture :", e)
        df = None
    finally:
        print("Fin de la tâche Extract")
    return df

# Transformation (actuellement vide, à compléter selon les besoins)
def Transform(data):
    # Exemple de transformation : renommer une colonne
    # data.rename(columns={'AncienNom': 'NouveauNom'}, inplace=True)
    return data

# Chargement
def Load(data, path):
    try:
        data.to_excel(path, index=False, engine="openpyxl")
        print("Chargement réussi")
    except Exception as e:
        print("Echec du chargement :", e)
    finally:
        print("Fin de la tâche Load")

# Processus ETL
def ETL():
    # Remplacer l'URL par le lien brut du fichier CSV sur GitHub
    url = "https://raw.githubusercontent.com/J-Y-KPANGBAN/DATAhouse/main/SampleSuperstore.csv"
    # Définir le chemin de destination local pour le fichier Excel
    path = "data_LIDL_V1load.xlsx"
    
    # Extraction
    data = Extract(url)
    
    # Vérification si l'extraction s'est bien déroulée
    if data is not None:
        print("Extraction réussie")
        
        # Transformation (si nécessaire)
        data = Transform(data)
        
        # Chargement
        Load(data, path)
    else:
        print("L'opération ETL est annulée en raison d'une extraction échouée")
    
    print("Fin de la tâche ETL")

# Exécution du processus ETL
ETL()
