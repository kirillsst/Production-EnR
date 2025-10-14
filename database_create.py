from supabase import create_client, Client
from sqlalchemy import create_engine, MetaData, Table, Column, Float, Integer, DateTime, text
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv
import pandas as pd
import pathlib as pl
import os

load_dotenv()
TYPES = os.getenv("types")

class Database():
    def __init__(self, url:str, service_key: str, database_url: str, energy_type: str = None):
        if energy_type not in (None, TYPES):
            raise ValueError(f"energy_type doit être parmi {TYPES} ou None pour tous, reçu: {energy_type}")
        self.energy_type = energy_type
        self.engine = create_engine(database_url, poolclass=NullPool)
        self.client = create_client(url, service_key)
        self.meta = MetaData()
        self.solaire_table = None
        self.eolienne_table = None
        self.hydro_table = None
    
    def create_table(self):
        if self.energy_type in (None, "solaire"):
            self.solaire_table = Table(
                "Solaire_data",
                self.meta,
                Column('id', Integer, primary_key=True),
                Column('date', DateTime, nullable=True, unique=True),
                Column('prod_solaire', Float, nullable=True),
                Column('global_tilted_irradiance', Float, nullable=True),
                Column('temperature_2m', Float, nullable=True)
                )
        if self.energy_type in (None, "eolienne"):
            self.eolienne_table = Table(
                "Eolienne_data",
                self.meta,
                Column('id', Integer, primary_key=True),
                Column('date', DateTime, nullable=True, unique=True),
                Column('prod_eolienne', Float, nullable=True),
                Column('wind_speed_10m_mean', Float, nullable=True),
                Column('pressure_msl_mean', Float, nullable=True),
                Column('temperature_2m_mean', Float, nullable=True)
                )
        if self.energy_type in (None, "hydro"):
            self.hydro_table = Table(
                "Hydro_data",
                self.meta,
                Column('id', Integer, primary_key=True),
                Column('date', DateTime, nullable=True, unique=True),
                Column('prod_hydro', Float, nullable=True),
                Column('QmnJ', Float, nullable=True),
                Column('HIXnJ', Float, nullable=True)
            )
            
        self.meta.create_all(self.engine)

        with self.engine.begin() as conn:
            conn.execute(text(f'ALTER TABLE "{"public"}"."{self.solaire_table}" ENABLE ROW LEVEL SECURITY;'))
            conn.execute(text(f'ALTER TABLE "{"public"}"."{self.eolienne_table}" ENABLE ROW LEVEL SECURITY;'))
            conn.execute(text(f'ALTER TABLE "{"public"}"."{self.hydro_table}" ENABLE ROW LEVEL SECURITY;'))

    def drop_table(self):
        with self.engine.begin() as conn:
            if self.energy_type in (None, "solaire"):
                if self.solaire_table is None:
                    self.solaire_table = Table("Solaire_data", self.meta, autoload_with=self.engine)
                    self.solaire_table.drop(self.engine, checkfirst=True)
            if self.energy_type in (None, "eolienne"):
                if self.eolienne_table is None:
                    self.eolienne_table = Table("Eolienne_data", self.meta, autoload_with=self.engine)
                    self.eolienne_table.drop(self.engine, checkfirst=True)
            if self.energy_type in (None, "hydro"):
                if self.hydro_table is None:
                    self.hydro_table = Table("Hydro_data", self.meta, autoload_with=self.engine)
                    self.hydro_table.drop(self.engine, checkfirst=True)

    def fetch_data(self, file_path:str = os.path.join(os.getcwd(), "data/train/")):
        if os.access(file_path, os.F_OK) is False:
            os.makedirs(file_path)
        if self.energy_type in (None, "solaire"):
            if self.solaire_table is None:
                with self.engine.begin() as conn:
                    self.solaire_table = Table("Solaire_data", self.meta, autoload_with=self.engine)
                    result_solaire = conn.execute(self.solaire_table.select())
                    df_solaire = pd.DataFrame(sorted(result_solaire))
                    df_solaire.to_csv(file_path+"solaire_train.csv")
        if self.energy_type in (None, "eolienne"):
            if self.eolienne_table is None:
                with self.engine.begin() as conn:
                    self.eolienne_table = Table("Eolienne_data", self.meta, autoload_with=self.engine)
                    result_eolienne = conn.execute(self.eolienne_table.select())
                    df_eolienne = pd.DataFrame(sorted(result_eolienne))
                    df_eolienne.to_csv(file_path+"eolienne_train.csv")
        if self.energy_type in (None, "hydro"):
            if self.hydro_table is None:
                with self.engine.begin() as conn:
                    self.hydro_table = Table("Hydro_data", self.meta, autoload_with=self.engine)
                    result_hydro = conn.execute(self.hydro_table.select())
                    df_hydro = pd.DataFrame(sorted(result_hydro))
                    df_hydro.to_csv(file_path+"hydro_train.csv")