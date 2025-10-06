from supabase import create_client
from dotenv import load_dotenv
from dataholder import CSVDataHandler
import os
import pandas as pd

# Exemple client Supabase à utiliser dans le client de la classe
load_dotenv()

URL = os.getenv("SUPABASE_URL")
SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

solar_data = CSVDataHandler(URL, SERVICE_ROLE_KEY, "solaire", "data/prod_solaire.csv")
solar_data.save_to_db("Solaire_data")

eolienne_data = CSVDataHandler(URL, SERVICE_ROLE_KEY, "eolienne", "data/prod_eolienne.csv")
eolienne_data.save_to_db("Eolienne_data")

hydro_data = CSVDataHandler(URL, SERVICE_ROLE_KEY, "hydro", "data/prod_hydro.csv")
hydro_data.save_to_db("Hydro_data")