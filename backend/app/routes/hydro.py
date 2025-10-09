from fastapi import APIRouter
from pydantic import BaseModel
import pandas as pd
from app.model_trainer import ModelTrain

router = APIRouter()

class HydroInput(BaseModel):
    QmnJ: float
    HIXnJ: float

@router.post("/predict/hydro")
def predict_hydro(data: HydroInput):
    df = pd.DataFrame([data.model_dump()])
    model = ModelTrain.load("hydro", ["QmnJ", "HIXnJ"], "prod_hydro")
    prediction = model.predict(df)[0]
    return {"prediction": prediction}
