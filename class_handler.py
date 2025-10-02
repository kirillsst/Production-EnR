from abc import ABC, abstractmethod
from supabase import create_client
import pandas as pd
from dotenv import

SUPABASE_URL
SUPABASE_KEY
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# Classe abstraite
class DataHandler(ABC):
  def __init__(self, client):
      """Charge les données et retourne un DataFrame"""
      self.client = client
  @abstractmethod
  def load(self) -> pd.DataFrame:
      """Charge les données et retourne un DataFrame"""
      pass




  def save_to_db(self, table_name: str):
      """Sauvegarde le DataFrame dans Supabase"""
      df = self.load()
      records = df.to_dict(orient="records")
      response = self.client.table(table_name).insert(records).execute()

class ProdHydroHandler(DataHandler):
    def __init__(self, client, prod_path, hydro_folder):
        pass