from supabase import create_client, Client
from abc import ABC, abstractmethod
from dotenv import load_dotenv
from retry_requests import retry
import pandas as pd
import requests
import openmeteo_requests
import requests_cache
import os

# Exemple client Supabase à utiliser dans le client de la classe

load_dotenv()
TYPES = os.getenv("types")
CODE_ENTITY = os.getenv("code_entite")
HYDRO_API_URL = os.getenv("hydro_api_url")

# Classe abstraite
class DataHandler(ABC):
    def __init__(self, url: str, service_key: str, energy_type: str = None):
      if energy_type is not None and energy_type not in TYPES:
            raise ValueError(f"energy_type doit être parmi {TYPES} ou None pour tous, reçu: {energy_type}")
      self.client = create_client(url, service_key)
      self.energy_type = energy_type
    
    @abstractmethod
    def load(self) -> pd.DataFrame:
      """Charge les données et retourne un DataFrame"""
      pass
  
    @abstractmethod
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
      """Nettoie les données et retourne un Dataframe"""
      pass
    
    def save_to_db(self, table_name: str):
      """Sauvegarde le DataFrame dans Supabase"""
      df = self.load()
      df = self.clean(df)
      records = df.to_dict(orient="records")
      self.client.table(table_name).delete().neq("id", -1).execute()
      response = (self.client.table(table_name).upsert(records, on_conflict="date").execute())
      return response

class CSVDataHandler(DataHandler):
    def __init__(self, url, service_key, energy_type, path: str):
       super().__init__(url, service_key, energy_type)
       self.path = path

    def load(self) -> pd.DataFrame:
       return pd.read_csv(self.path)

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
      df.iloc[:, 1] = df.iloc[:, 1].abs()
      if self.energy_type == "hydro":
        df = df.loc[(df.iloc[:, 1] <= 200)  & (df.iloc[:, 1] > 0)].copy()
        df = df.rename(columns={"date_obs_elab" : "date"})
      elif self.energy_type == "eolienne":
        df = df.loc[(df.iloc[:, 1] <= 100)  & (df.iloc[:, 1] > 0)].copy()
      elif self.energy_type == "solaire":
        df = df.loc[(df.iloc[:, 1] <= 100)  & (df.iloc[:, 1] > 0)].copy()
        df.iloc[:, 1] = df.iloc[:, 1]*1.5
      df["date"] = pd.to_datetime(df["date"])
      df = df.sort_values(by="date")
      df = df.drop_duplicates(subset="date", keep="first")
      df = df.dropna().reset_index(drop=True)
      df["date"] = df["date"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
      return df


class APIDataHandler(DataHandler):
    def __init__(self, url, service_key, energy_type, parameters: list):
        super().__init__(url, service_key, energy_type)
        self.parameters = parameters

    def load(self) -> pd.DataFrame:
        
        if self.energy_type == "hydro":
          all_data = []
          for param in self.parameters:
              params = {"code_entite": self.service_key, "grandeur_hydro_elab": param, "size": 1500}
              response = requests.get(self.url, params=params)
              df = pd.DataFrame(response.json().get("data", []))
              if not df.empty:
                  df["grandeur_hydro_elab"] = param
                  all_data.append(df)
          if all_data:
              return pd.concat(all_data, ignore_index=True)
          return pd.DataFrame()

#     def clean(self, df_api: pd.DataFrame) -> pd.DataFrame:
#         if df_api.empty:
#             print("Aucune donnée API")
#             return pd.DataFrame()

#         df_api["date"] = pd.to_datetime(df_api["date_obs_elab"]).dt.strftime("%Y-%m-%d")
#         df_api = df_api[["date", "grandeur_hydro_elab", "resultat_obs_elab"]]

#         df_pivot = df_api.pivot_table(
#             index="date",
#             columns="grandeur_hydro_elab",
#             values="resultat_obs_elab",
#             aggfunc="mean"
#         ).reset_index()

#         df_prod = self.csv_handler.load()
#         df_prod = self.csv_handler.clean(df_prod)

#         df_full = pd.merge(df_prod, df_pivot, on="date", how="left")

#         #with traitement
#         colonnes_grandeurs = self.grandeurs  # ["QmnJ", "HIXnJ"]
#         df_full = df_full[df_full[colonnes_grandeurs].fillna(0).sum(axis=1) > 0]
#         colonnes_a_remplir = ["prod_hydro"] + colonnes_grandeurs
#         df_full[colonnes_a_remplir] = df_full[colonnes_a_remplir].fillna(0)
        

#         # without traitement
#         # df_full[self.grandeurs] = df_full[self.grandeurs].fillna(0)
  
#         #with traitement
#         # Traitement les valeurs aberrantes
#         df_full = df_full[
#           (df_full["QmnJ"] > 0) & (df_full["QmnJ"] < 10000) &
#           (df_full["HIXnJ"] > 0) & (df_full["HIXnJ"] < 2000)
#         ]

#         for col in ["QmnJ", "HIXnJ"]:
#           Q1 = df_full[col].quantile(0.25)
#           Q3 = df_full[col].quantile(0.75)
#           IQR = Q3 - Q1
#           df_full = df_full[
#               (df_full[col] >= Q1 - 1.5 * IQR) & 
#               (df_full[col] <= Q3 + 1.5 * IQR)
#           ]

#         df_full = df_full.reset_index(drop=True)
#         df_full.insert(0, "id", df_full.index + 1)

#         return df_full

#     def save_to_bd(self, nom_table: str) -> int:
#         df_api = self.load_api()
#         df_clean = self.clean(df_api)
#         if df_clean.empty:
#             print("Aucune donnée à insérer dans la base")
#             return 0
#         enregistrements = df_clean.to_dict(orient="records")
#         self.client.table(nom_table).upsert(enregistrements, on_conflict="date").execute()
#         print(f"{len(enregistrements)} enregistrements insérés/mis à jour dans {nom_table}")
#         return len(enregistrements)


# if __name__ == "__main__":
#     hydro_handler = HydroHandler(
#         supabase,
#         code_entite="Y321002101",
#         grandeurs=["QmnJ", "HIXnJ"],
#         chemin_csv_prod="data/prod_hydro.csv"
#     )
#     hydro_handler.save_to_bd("Hydro_data")



cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://archive-api.open-meteo.com/v1/archive"
params = {
	"latitude": 43.6109,
	"longitude": 3.8763,
	"start_date": "2016-09-01",
	"end_date": "2025-09-25",
	"hourly": ["global_tilted_irradiance", "temperature_2m", "wind_speed_50m"],
	"tilt": 35,
}
responses = openmeteo.weather_api(url, params=params)
response = responses[0]
hourly = response.Hourly()
hourly_global_tilted_irradiance = hourly.Variables(0).ValuesAsNumpy()
hourly_temperature_2m = hourly.Variables(1).ValuesAsNumpy()
hourly_wind_speed_50m = hourly.Variables(2).ValuesAsNumpy()
