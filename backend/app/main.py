from fastapi import FastAPI
from app.routes import hydro, solaire, eolienne

app = FastAPI(title="API production EnR")

app.include_router(hydro.router, prefix="/hydro", tags=["Hydro"])
app.include_router(solaire.router, prefix="/solaire", tags=["Solaire"])
app.include_router(eolienne.router, prefix="/eolienne", tags=["Eolienne"])