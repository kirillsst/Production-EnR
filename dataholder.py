from supabase import create_client, Client
from abc import ABC, abstractmethod
from dotenv import load_dotenv
from retry_requests import retry
import pandas as pd
import openmeteo_requests
import requests_cache
import os

# Exemple client Supabase à utiliser dans le client de la classe

load_dotenv()
TYPES = os.getenv("types")

# Classe abstraite
class DataHandler(ABC):
    def __init__(self, url: str, service_key: str, energy_type: str):
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
      if self.energy_type not in TYPES:
        print(f"Please enter any of types in this list : {TYPES} ")
      elif self.energy_type == "hydro":
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
      
# class APIDataHandler(DataHandler):
#     def __init__(self, url, service_key, energy_type):
#        super().__init__(url, service_key, energy_type)
       
