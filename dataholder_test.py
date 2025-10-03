from supabase import create_client, Client
import pandas as pd
from dotenv import load_dotenv
import os
from dataholder import CSVDataHandler

# Exemple client Supabase à utiliser dans le client de la classe
load_dotenv()
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase = create_client(url, key)

solar_data = CSVDataHandler(supabase, "solar", "data/prod_solaire_test.csv")
solar_data.save_to_db("Solar_data")