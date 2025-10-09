from supabase import create_client
from dotenv import load_dotenv
from dataholder import CSVDataHandler
import os
import pandas as pd

# Exemple client Supabase à utiliser dans le client de la classe
load_dotenv()

URL = os.getenv("SUPABASE_URL")
SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

solar_data = CSVDataHandler(URL, SERVICE_ROLE_KEY, "solaire", "data/raw/prod_solaire_new.csv")
solar_data.save_to_db("Solaire_data")

eolienne_data = CSVDataHandler(URL, SERVICE_ROLE_KEY, "eolienne", "data/raw/prod_eolienne_new.csv")
eolienne_data.save_to_db("Eolienne_data")

hydro_data = CSVDataHandler(URL, SERVICE_ROLE_KEY, "hydro", "data/raw/prod_hydro_new.csv")
hydro_data.save_to_db("Hydro_data")