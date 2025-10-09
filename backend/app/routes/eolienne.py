# backend/app/routes/hydro.py
from fastapi import APIRouter
from pydantic import BaseModel
import pandas as pd
from app.model_trainer import ModelTrain

router = APIRouter()

class EolienneInput(BaseModel):
    windspeed_10m: float
    wind_speed3: float
    temp_press: float

@router.post("/predict/wind")
def predict_wind(data: EolienneInput):
    df = pd.DataFrame([data.model_dump()])
    model = ModelTrain.load("eolienne", ["windspeed_10m", "wind_speed3", "temp_press"], "prod_eolienne")
    prediction = model.predict(df)[0]
    return {"prediction": prediction}