from fastapi import APIRouter
from pydantic import BaseModel
import pandas as pd
from app.model_trainer import ModelTrain


router = APIRouter()

class EolienneInput(BaseModel):
    wind_speed_10m_mean: float
    pressure_msl_mean: float
    temperature_2m_mean: float

@router.post("/predict/eolienne")
def predict_wind(data: EolienneInput):
    if data.wind_speed_10m_mean == 0 or data.pressure_msl_mean == 0 or data.temperature_2m_mean == 0:
        return {"error": " wind_speed_10m_mean, pressure_msl_mean et temperature_2m_mean devraient être plus nombreux 0"}
    
    df = pd.DataFrame([data.model_dump()])
    # Création des features 
    df['wind_speed3'] = df['wind_speed_10m_mean']**3
    df['temp_press'] = df['temperature_2m_mean']*df['pressure_msl_mean']
    
    model = ModelTrain.load("eolienne", ["wind_speed_10m_mean", "pressure_msl_mean", "temperature_2m_mean", "wind_speed3", "temp_press"], "prod_eolienne")
    prediction = model.predict(df)[0]
    return {"prediction": prediction}