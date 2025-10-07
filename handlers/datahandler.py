from abc import ABC, abstractmethod
from datetime import datetime
import os
import pandas as pd
import requests
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TYPES = ("hydro", "eolienne", "solaire")
API_URL = "https://hubeau.eaufrance.fr/api/v2/hydrometrie/obs_elab"

class DataHandler(ABC):
    def __init__(self, client: Client, type_energie: str):
        self.client = client
        self.type_energie = type_energie

    @abstractmethod
    def load(self) -> pd.DataFrame:
        """Charger les données en DataFrame"""
        pass

    @abstractmethod
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Nettoyer et filtrer les données"""
        pass

    def save_to_bd(self, table_name: str) -> int:
        """Sauvegarde les données dans Supabase et retourne le nombre de lignes"""
        df = self.charger()
        df = self.nettoyer(df)
        df = df.astype(object).where(pd.notnull(df), None)
        enregistrements = df.to_dict(orient="records")
        if not enregistrements:
            print("Aucune donnée à insérer.")
            return 0
        self.client.table(table_name).upsert(enregistrements, on_conflict="date").execute()
        print(f"{len(enregistrements)} enregistrements insérés/mis à jour dans {table_name}")
        return len(enregistrements)

class CSVDataHandler(DataHandler):
    def __init__(self, client: Client, type_energie: str, chemin: str):
        super().__init__(client, type_energie)
        self.chemin = chemin

    def load(self) -> pd.DataFrame:
        return pd.read_csv(self.chemin)

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.type_energie not in TYPES:
            raise ValueError(f"Le type d'énergie doit être dans {TYPES}")
        
        df = df.copy()
        valeur_col = "prod_hydro"
        df[valeur_col] = df[valeur_col].abs()

        if self.type_energie == "hydro":
            df = df[(df[valeur_col] > 0) & (df[valeur_col] <= 200)]
        elif self.type_energie == "eolienne":
            df = df[(df[valeur_col] > 0) & (df[valeur_col] <= 100)]
        elif self.type_energie == "solaire":
            df = df[(df[valeur_col] > 0) & (df[valeur_col] <= 100)]
            df[valeur_col] *= 1.5

        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        df = df.dropna(subset=[valeur_col, "date"])
        return df[["date", valeur_col]]


class HydroHandler:
    def __init__(self, client: Client, code_entite: str, grandeurs: list, chemin_csv_prod: str):
        self.client = client
        self.code_entite = code_entite
        self.grandeurs = grandeurs
        self.csv_handler = CSVDataHandler(client, "hydro", chemin_csv_prod)

    def load_api(self) -> pd.DataFrame:
        toutes_donnees = []
        for grandeur in self.grandeurs:
            params = {"code_entite": self.code_entite, "grandeur_hydro_elab": grandeur, "size": 500}
            response = requests.get(API_URL, params=params)
            response.raise_for_status()
            df = pd.DataFrame(response.json().get("data", []))
            if not df.empty:
                df["grandeur_hydro_elab"] = grandeur
                toutes_donnees.append(df)
        if toutes_donnees:
            return pd.concat(toutes_donnees, ignore_index=True)
        return pd.DataFrame()

    def clean(self, df_api: pd.DataFrame) -> pd.DataFrame:
        if df_api.empty:
            print("Aucune donnée API")
            return pd.DataFrame()

        df_api["date"] = pd.to_datetime(df_api["date_obs_elab"]).dt.strftime("%Y-%m-%d")
        df_api = df_api[["date", "grandeur_hydro_elab", "resultat_obs_elab"]]

        df_pivot = df_api.pivot_table(
            index="date",
            columns="grandeur_hydro_elab",
            values="resultat_obs_elab",
            aggfunc="mean"
        ).reset_index()

        df_prod = self.csv_handler.load()
        df_prod = self.csv_handler.clean(df_prod)

        df_full = pd.merge(df_prod, df_pivot, on="date", how="left")

        colonnes_grandeurs = self.grandeurs  # ["QmnJ", "HIXnJ"]
        df_full = df_full[df_full[colonnes_grandeurs].fillna(0).sum(axis=1) > 0]
        colonnes_a_remplir = ["prod_hydro"] + colonnes_grandeurs
        df_full[colonnes_a_remplir] = df_full[colonnes_a_remplir].fillna(0)

        return df_full

    def save_to_bd(self, nom_table: str) -> int:
        df_api = self.load_api()
        df_clean = self.clean(df_api)
        if df_clean.empty:
            print("Aucune donnée à insérer dans la base")
            return 0
        enregistrements = df_clean.to_dict(orient="records")
        self.client.table(nom_table).upsert(enregistrements, on_conflict="date").execute()
        print(f"{len(enregistrements)} enregistrements insérés/mis à jour dans {nom_table}")
        return len(enregistrements)


if __name__ == "__main__":
    hydro_handler = HydroHandler(
        supabase,
        code_entite="Y321002101",
        grandeurs=["QmnJ", "HIXnJ"],
        chemin_csv_prod="data/prod_hydro.csv"
    )
    hydro_handler.save_to_bd("Hydro_data")
