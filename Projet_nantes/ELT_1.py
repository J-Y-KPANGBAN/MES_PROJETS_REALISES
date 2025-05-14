
import pandas as pd
import requests as rq

def extraict(url):
    reponse = rq.get(url)
    if reponse.ok:
        data = reponse.json()
        df= pd.DataFrame(data['results'])
    else:
        print(f"error{reponse.status_code}")
    
    return df

def transform(df):
    df_commune= df[['commune', 'adresse','code_postal']]
    return pd.DataFrame(df_commune)

def chargement(data):
    data.to_csv("D_warehouse/data_commune.csv",index=False)
    return data


url ="https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_parcs-relais-nantes-metropole/records?limit=100"
df =extraict(url)
#print(f"{df.head(5)}")

#print(df.columns)

data_new = transform(df)

charge = chargement(data_new)
print(charge.head(5))

