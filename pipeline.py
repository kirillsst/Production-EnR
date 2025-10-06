from supabase import create_client, Client
from abc import ABC, abstractmethod
from retry_requests import retry
import pandas as pd
import openmeteo_requests
import requests_cache
import os
from dotenv import load_dotenv

load_dotenv()
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase = create_client(url, key)
TYPES = ("hydro", "eolienne", "solar")