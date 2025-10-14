from fastapi import FastAPI
from app.routes import hydro, solaire, eolienne

app = FastAPI(title="API production EnR")

app.include_router(hydro.router, tags=["Hydro"])
app.include_router(solaire.router, tags=["Solaire"])
app.include_router(eolienne.router, tags=["Eolienne"])