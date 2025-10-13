# backend/app/train_model.py
import os
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client
from model_trainer import ModelTrain
import argparse


def main(energy_type: str):
    """
    Script d'entraînement pour différents types d'énergie :
    - hydro
    - eolienne
    - solaire
    """

    print(f"--- Démarrage de l'entraînement du modèle pour : {energy_type.upper()} ---")

    # Chargement des variables d'environnement
    load_dotenv()
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Variables d'environnement manquantes : SUPABASE_URL ou SUPABASE_SERVICE_ROLE_KEY")

    # Connexion à Supabase
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Dictionnaire de configuration
    ENERGY_CONFIG = {
        "hydro": {
            "table": "Hydro_data",
            "features": ["QmnJ", "HIXnJ"],
            "target": "prod_hydro",
        },
        "eolienne": {
            "table": "Eolienne_data",
            "features": ["wind_speed3", "temp_press", "wind_speed_10m_mean (km/h)"],
            "target": "prod_eolienne",
        },
        "solaire": {
            "table": "Solaire_data",
            "features": ["irradiance", "temperature"],
            "target": "prod_solaire",
        },
    }

    if energy_type not in ENERGY_CONFIG:
        raise ValueError(f"Type d'énergie non reconnu : {energy_type}. Choisir parmi {list(ENERGY_CONFIG.keys())}")

    config = ENERGY_CONFIG[energy_type]

    # Chargement des données depuis Supabase
    print(f"Chargement des données depuis la table {config['table']} ...")
    data = supabase.table(config["table"]).select("*").execute()
    df = pd.DataFrame(data.data)

    if df.empty:
        print("Aucune donnée trouvée pour ce type d'énergie. Entraînement annulé.")
        return

    print(f"{len(df)} lignes chargées depuis Supabase.")

    # Entraînement du modèle
    print(f"Entraînement du modèle pour {energy_type.upper()}...")
    trainer = ModelTrain(
        producer_type=energy_type,
        features=config["features"],
        target=config["target"]
    )
    trainer.train(df)
    print(f"Modèle {energy_type.upper()} entraîné et sauvegardé avec succès !")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script d'entraînement pour les modèles d'énergie.")
    parser.add_argument(
        "energy_type",
        type=str,
        choices=["hydro", "eolienne", "solaire"],
        help="Type d'énergie à entraîner"
    )
    args = parser.parse_args()
    main(args.energy_type)
