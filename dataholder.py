from supabase import create_client
from abc import ABC, abstractmethod
import pandas as pd


# Exemple client Supabase à utiliser dans le client de la classe
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
types = ["hydro", "eolienne", "solar"]

# Classe abstraite
class DataHandler(ABC):
    def __init__(self, client):
      """Charge les données et retourne un DataFrame"""
      self.client = client
    
    @abstractmethod
    def load(self, client) -> pd.DataFrame:
      """Charge les données et retourne un DataFrame"""
      pass
  
    @abstractmethod
    def clean(self, df: pd.DataFrame) -> pd.Dataframe:
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
    def __init__(self, client, filepath: str, type: str):
       super().__init__(client)
       self.filepath = filepath
       self.type = type

    def load(self) -> pd.DataFrame:
       return pd.read_csv(self.filepath)

    def clean(self, type: str, df: pd.Dataframe) -> pd.DataFrame:
      df.iloc[:, 1].abs()
      if self.type not in types:
        print(f"Please enter any of types in this list : {types} ")
      elif type == "hydro":
        df.loc[(df.iloc[:, 1] < 200)  & (df.iloc[:, 1] > 0)]
      elif type == "eolienne" or "solar":
        df.loc[(df.iloc[:, 1] < 100)  & (df.iloc[:, 1] > 0)]
      df["date"] = pd.to_datetime(df["date"])
      df.dropna()
      return df
      
class DBDDataHandler(DataHandler):
   def __init__(self, url: str):
      self.url = url
  
   def load(self, url: str):
      pass 
       
