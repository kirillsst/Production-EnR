# backend/app/train_hydro_model.py
import pandas as pd
from model_trainer import ModelTrain
from dotenv import load_dotenv
from supabase import create_client
import os 

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
data = supabase.table("Hydro_data").select("*").execute()
df = pd.DataFrame(data.data)

trainer = ModelTrain(
    producer_type="hydro",
    features=["QmnJ", "HIXnJ"],
    target="prod_hydro"
)

trainer.train(df)
print("Hydro model trained and saved!")
