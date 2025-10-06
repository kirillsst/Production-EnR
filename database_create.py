from supabase import create_client, Client
from sqlalchemy import create_engine, MetaData, Table, Column, Float, Integer, DateTime
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv
import os

load_dotenv()
TYPES = os.getenv("types")
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")
URL = os.getenv("SUPABASE_URL")
SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")


DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"

class Database():
    def __init__(self, url:str, service_key: str, database_url: str, schema_name: str = "default", energy_type: str = None):
        self.engine = create_engine(database_url, poolclass=NullPool)
        self.client = create_client(url, service_key)
        self.meta = MetaData(schema=schema_name)
        self.energy_type = energy_type
    
    def Create(self):
        if self.energy_type not in TYPES and not None:
            print(f"Please enter any of types in this list : {TYPES} ")
            if self.energy_type == "solaire" or None:
                Solaire_data = Table(
                    "Solaire_data",
                    self.meta,
                    Column('id', Integer, primary_key=True),
                    Column('date', DateTime, nullable=True, unique=True),
                    Column('prod_solaire', Float, nullable=True),
                    Column('irradiance', Float, nullable=True),
                    Column('temperature', Float, nullable=True),
                    schema = self.schema_name
                )
            if self.energy_type == "eolienne" or None:
                Eolienne_data = Table(
                    "Eolienne_data",
                    self.meta,
                    Column('id', Integer, primary_key=True),
                    Column('date', DateTime, nullable=True, unique=True),
                    Column('prod_eolienne', Float, nullable=True),
                    Column('windspeed', Float, nullable=True),
                    Column('temperature', Float, nullable=True),
                    Column('wind_dir_sin', Float, nullable=True),
                    Column('wind_dir_con', Float, nullable=True),
                    schema = self.schema_name
                )
            if self.energy_type == "hydro" or None:
                Hydro_data = Table(
                    "Hydro_data",
                    self.meta,
                    Column('id', Integer, primary_key=True),
                    Column('date', DateTime, nullable=True, unique=True),
                    Column('prod_hydro', Float, nullable=True),
                    Column('QmnJ', Float, nullable=True),
                    Column('QmM', Float, nullable=True),
                    Column('HIXnJ', Float, nullable=True),
                    Column('HIXM', Float, nullable=True),
                    schema = self.schema_name
                )
            
        self.meta.create_all(self.engine)

    def Drop(self):
        with self.engine.connect() as conn:
            if self.energy_type == "solaire" or None:
                conn.execute(Solaire_data.drop())
                conn.commit()
            if self.energy_type == "eolienne" or None:
                conn.execute(Eolienne_data.drop())
                conn.commit()
            if self.energy_type == "hydro" or None:
                conn.execute(Solaire_data.drop())
                conn.commit()

    def Fetch(self):
        if self.energy_type == "solaire" or None:
            response = (self.client.table("Solaire_data").select("*").execute())
            return response
        if self.energy_type == "eolienne" or None:
            response = (self.client.table("Eolienne_data").select("*").execute())
            return response
        if self.energy_type == "hydro" or None:
            response = (self.client.table("Hydro_data").select("*").execute())
            return response