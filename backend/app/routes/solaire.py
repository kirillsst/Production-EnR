from fastapi import APIRouter
from pydantic import BaseModel
import pandas as pd
from app.model_trainer import ModelTrain

router = APIRouter()

class SolaireInput(BaseModel):
    global_tilted_irradiance: float
    temperature_2m: float

@router.post("/predict/solaire")
def predict_solar(data: SolaireInput):
    if data.global_tilted_irradiance == 0 or data.temperature_2m == 0:
        return {"error": "global_tilted_irradiance et temperature_2m doit être supérieur à 0"}
    
    df = pd.DataFrame([data.model_dump()])
    model = ModelTrain.load("solaire", ["global_tilted_irradiance", "temperature_2m"], "prod_solaire")
    prediction = model.predict(df)[0]
    return {"prediction": prediction}
