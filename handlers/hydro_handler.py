import pandas as pd 
import requests
import numpy as np

API_URL = "https://hubeau.eaufrance.fr/api/v2/hydrometrie/obs_elab"

class HydroDataHandler:
    def __init__(self, client, code_entite: str, grandeurs: list):
        self.client = client
        self.code_entite = code_entite
        self.grandeurs = grandeurs

    def load(self) -> pd.DataFrame:
        """Télécharge toutes les données pour chaque valeur hydro"""
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
        """Filtre et convertit les données dans un format avec des colonnes par valeur"""
        df = df[
            (df["code_statut"] == 4) &  # Donnée brute
            (df["code_methode"] == 0) &  # Mesurée
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

        # Ajoutez id et prod_hydro (si vous ne les avez pas encore, vous pouvez indiquer None).
        pivot_df.insert(0, "id", range(1, len(pivot_df) + 1))
        pivot_df.insert(1, "prod_hydro", None)

        # Rennomer date_obs_elab → date
        pivot_df.rename(columns={"date_obs_elab": "date"}, inplace=True)

        print(f"Données nettoyées et pivotées : {len(pivot_df)} lignes")
        print(df.dtypes)
        return pivot_df

    def save_to_db(self, table_name: str):
      df = self.load()
      df = self.clean(df)

      df = df.replace([np.inf, -np.inf], np.nan)
      df = df.where(pd.notnull(df), None)

      df = df.astype(object).where(pd.notnull(df), None)

      records = df.to_dict(orient="records")

      records = [r for r in records if any(v is not None for v in r.values())]

      print(f"Insertion dans Supabase ({len(records)} lignes)...")

      response = self.client.table(table_name).insert(records).execute()
      print(f"Données insérées dans {table_name}")
      return response