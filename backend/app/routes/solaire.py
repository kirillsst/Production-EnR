# backend/app/routes/hydro.py
from fastapi import APIRouter
from pydantic import BaseModel
import pandas as pd
from app.model_trainer import ModelTrain

router = APIRouter()

class SolaireInput(BaseModel):
    irradiance: float
    temperature: float

@router.post("/predict/solaire")
def predict_solar(data: SolaireInput):
    df = pd.DataFrame([data.model_dump()])
    model = ModelTrain.load("solaire", ["irradiance", "temperature"], "prod_solaire")
    prediction = model.predict(df)[0]
    return {"prediction": prediction}