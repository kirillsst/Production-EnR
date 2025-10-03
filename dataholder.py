from supabase import create_client, Client
from abc import ABC, abstractmethod
import pandas as pd
from dotenv import load_dotenv
import os

# Exemple client Supabase à utiliser dans le client de la classe
load_dotenv()
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase = create_client(url, key)
TYPES = ("hydro", "eolienne", "solar")

# Classe abstraite
class DataHandler(ABC):
    def __init__(self, client, energy_type: str):
      self.client = client
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
      response = self.client.table(table_name).insert(records).execute()
      return response

class CSVDataHandler(DataHandler):
    def __init__(self, client, energy_type, path: str):
       super().__init__(client, energy_type)
       self.path = path

    def load(self) -> pd.DataFrame:
       return pd.read_csv(self.path)

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
      df.iloc[:, 1] = df.iloc[:, 1].abs()
      if self.energy_type not in TYPES:
        print(f"Please enter any of types in this list : {TYPES} ")
      elif self.energy_type == "hydro":
        df = df.loc[(df.iloc[:, 1] <= 200)  & (df.iloc[:, 1] > 0)].copy()
      elif self.energy_type == "eolienne":
        df = df.loc[(df.iloc[:, 1] <= 100)  & (df.iloc[:, 1] > 0)].copy()
      elif self.energy_type == "solar":
        df = df.loc[(df.iloc[:, 1] <= 100)  & (df.iloc[:, 1] > 0)].copy()
        df.iloc[:, 1] = df.iloc[:, 1]*1.5
      df["date"] = pd.to_datetime(df["date"])
      df = df.dropna()
      df["date"] = df["date"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
      return df
      
# class DBDDataHandler(DataHandler):
#    def __init__(self, url: str):
#       self.url = url
  
   def load(self, url: str):
      pass 
       
