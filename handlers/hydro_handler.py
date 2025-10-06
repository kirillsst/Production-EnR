import pandas as pd 
import requests
import numpy as np

API_URL = "https://hubeau.eaufrance.fr/api/v2/hydrometrie/obs_elab"

class HydroDataHandler:
    def __init__(self, client, code_entite: str, grandeurs: list, prod_csv_path: str):
        self.client = client
        self.code_entite = code_entite
        self.grandeurs = grandeurs
        self.prod_csv_path = prod_csv_path  # path prod_hydro.csv

    def load(self) -> pd.DataFrame:
        """Télécharge toutes les données pour chaque grandeur hydro"""
        all_data = []
        for grandeur in self.grandeurs:
            params = {
                "code_entite": self.code_entite,
                "grandeur_hydro_elab": grandeur,
                "size": 500
            }
            response = requests.get(API_URL, params=params)
            response.raise_for_status()
            json_data = response.json()
            df = pd.DataFrame(json_data["data"])
            df["grandeur_hydro_elab"] = grandeur
            all_data.append(df)
        df_all = pd.concat(all_data, ignore_index=True)
        print(f"Données chargées : {len(df_all)} lignes")
        return df_all

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[
            (df["code_statut"] == 4) &
            (df["code_methode"] == 0) &
            (df["code_qualification"] == 16)
        ].copy()

        df["date_obs_elab"] = pd.to_datetime(df["date_obs_elab"]).dt.strftime("%Y-%m-%d")
        df = df[["date_obs_elab", "grandeur_hydro_elab", "resultat_obs_elab"]]

        pivot_df = df.pivot_table(
            index="date_obs_elab",
            columns="grandeur_hydro_elab",
            values="resultat_obs_elab",
            aggfunc="mean"
        ).reset_index()

        df_prod = pd.read_csv(self.prod_csv_path)
        df_prod.rename(columns={"date_obs_elab": "date"}, inplace=True)

        pivot_df.rename(columns={"date_obs_elab": "date"}, inplace=True)
        df_full = pd.merge(df_prod, pivot_df, on="date", how="left")

        for g in self.grandeurs:
            if g not in df_full.columns:
                df_full[g] = None

        df_full = df_full.where(pd.notnull(df_full), None)

        df_full.insert(0, "id", range(1, len(df_full) + 1))

        print(f"Données nettoyées et merge avec prod_hydro : {len(df_full)} lignes")
        return df_full

    def save_to_db(self, table_name: str):
        df = self.load()
        df = self.clean(df)

        df = df.astype(object).where(pd.notnull(df), None)

        records = df.to_dict(orient="records")
        records = [r for r in records if any(v is not None for v in r.values())]

        print(f"Insertion/upsert dans Supabase ({len(records)} lignes)...")
        for record in records:
            self.client.table(table_name).upsert(record, on_conflict="date").execute()
        print(f"Données insérées ou mises à jour dans {table_name}")
        return len(records)
