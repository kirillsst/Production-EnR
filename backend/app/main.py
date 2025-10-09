from fastapi import FastAPI
from app.routes import hydro

app = FastAPI(title="API production EnR")

app.include_router(hydro.router, prefix="/hydro", tags=["Hydro"])