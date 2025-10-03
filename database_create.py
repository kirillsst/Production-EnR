from supabase import create_client, Client
from sqlalchemy import create_engine, MetaData, Table, Column, Float, Integer, DateTime
from dotenv import load_dotenv
import os

load_dotenv()
url = os.getenv("DATABASE_URL")

engine = create_engine(url)

meta = MetaData()

Solar_data = Table(
    "Solar_data",
    meta,
    Column('id', Integer, primary_key=True),
    Column('date', DateTime, null=True),
    Column('prod_solar', Float, null=True),
    Column('irradiance', Float, null=True),
    Column('temperature', Float, null=True)
)

meta.create_all(engine)

conn= engine.connect()
