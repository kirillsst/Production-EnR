from supabase import create_client, Client
from sqlalchemy import create_engine, MetaData, Table, Column, Float, Integer, DateTime
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv
from backend.app.database_manager import Database
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

# solar_table = Database(URL, SERVICE_ROLE_KEY, DATABASE_URL, "Meteo_data", "solar")
# solar_table.Create()
all_tables = Database(URL, SERVICE_ROLE_KEY, DATABASE_URL)


# all_tables.create_tables()
# all_tables.fetch_data()
# all_tables.drop_tables()
all_tables.drop_na()