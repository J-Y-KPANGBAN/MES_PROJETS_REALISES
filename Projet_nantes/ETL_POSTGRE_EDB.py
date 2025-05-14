import sqlalchemy
import pandas as pd

def maconnect_EDb(url):
    try:
        # Créer l'engine SQLAlchemy
        engine = sqlalchemy.create_engine(url)
        # Essayer de se connecter pour valider la connexion
        with engine.connect() as connection:
            print("Connexion réussie ✅")
        return engine  # Retourner l'engine si tout va bien
    except sqlalchemy.exc.SQLAlchemyError as e:
        print(f"Erreur de connexion ❌ : {e}")
        return None  # Retourner None en cas d'erreur de connexion

# Exemple d'URL
url = "postgresql+psycopg2://root:ROOT@host:5432/Cinema"

# Connexion à la base
engine = maconnect_EDb(url)

if engine:
    # Lire les données de la table "films" si la connexion est établie
    df = pd.read_sql_table("films", con=engine)
    print(df.head())
else:
    print("Impossible de se connecter à la base de données.")
