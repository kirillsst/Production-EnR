from supabase import create_client, Client
from abc import ABC, abstractmethod
from dotenv import load_dotenv
from retry_requests import retry
from datetime import date
import pandas as pd
import requests
import openmeteo_requests
import requests_cache
import os

# Exemple client Supabase à utiliser dans le client de la classe

load_dotenv()
TYPES = os.getenv("types")
CODE_ENTITY = os.getenv("code_entite")
HYDRO_API_URL = os.getenv("hydro_api_url")

# Classe abstraite
class DataHandler(ABC):
    def __init__(self, url: str, service_key: str, energy_type: str = None):
      if energy_type is not None and energy_type not in TYPES:
            raise ValueError(f"energy_type doit être parmi {TYPES} ou None pour tous, reçu: {energy_type}")
      self.client = create_client(url, service_key)
      self.energy_type = energy_type
    
    @abstractmethod
    def load(self) -> pd.DataFrame:
      """Charge les données et retourne un DataFrame"""
      pass
  
    @abstractmethod
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
      """Nettoie les données et retourne un Dataframe"""
      pass
    
    def save_to_db(self, table_name: str):
      """Sauvegarde le DataFrame dans Supabase"""
      df = self.load()
      df = self.clean(df)
      records = df.to_dict(orient="records")
      response = (self.client.table(table_name).upsert(records, on_conflict="date").execute())
      return response

class CSVDataHandler(DataHandler):
    def __init__(self, url, service_key, energy_type, path: str):
       super().__init__(url, service_key, energy_type)
       self.path = path

    def load(self) -> pd.DataFrame:
       return pd.read_csv(self.path)

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        if self.energy_type == "hydro" and "date" not in df.columns and "date_obs_elab" in df.columns:
            df = df.rename(columns={"date_obs_elab": "date"})

        if "date" not in df.columns:
            if isinstance(df.index, (pd.DatetimeIndex, pd.PeriodIndex)):
                df = df.reset_index().rename(columns={"index": "date"})
            else:
                raise KeyError("La colonne 'date' est absente et l'index n'est pas temporel.")

        df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")

        prod_col_map = {
            "hydro": "prod_hydro",
            "eolienne": "prod_eolienne",
            "solaire": "prod_solaire",
        }
        if self.energy_type not in prod_col_map:
            raise ValueError(f"energy_type inconnu: {self.energy_type}")

        prod_col = prod_col_map[self.energy_type]
        if prod_col not in df.columns:
            raise KeyError(f"Colonne '{prod_col}' absente du DataFrame.")

        df[prod_col] = pd.to_numeric(df[prod_col], errors="coerce").abs()

        bounds_max = {"hydro": 200.0, "eolienne": 100.0, "solaire": 100.0}
        df = df[(df[prod_col] > 0) & (df[prod_col] <= bounds_max[self.energy_type])]

        if self.energy_type == "solaire":
            df[prod_col] = df[prod_col] * 1.5

        df = df.dropna(subset=["date", prod_col])
        df = df.sort_values("date").drop_duplicates("date", keep="first").reset_index(drop=True)
        df["date"] = df["date"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        return df[["date", prod_col]]



class APIDataHandler(DataHandler):
    def __init__(self, url, service_key, energy_type, api_url :str):
        super().__init__(url, service_key, energy_type)
        self.api_url = api_url

    def load(self) -> pd.DataFrame:
        
        if self.energy_type == "solaire":
            
            retry_session = retry(requests.Session(), retries=5, backoff_factor=0.2)
            openmeteo_solaire = openmeteo_requests.Client(session=retry_session)
            params_solaire = {
              "latitude": 43.6109,
              "longitude": 3.8763,
              "start_date": "2016-09-01",
              "end_date": "2025-09-29",
              "timezone": "UTC",
              "hourly": ["global_tilted_irradiance", "temperature_2m"],
              "tilt": 35,
            }
            responses = openmeteo_solaire.weather_api(self.api_url, params=params_solaire)
            hourly = responses[0].Hourly()
            hourly_global_tilted_irradiance = hourly.Variables(0).ValuesAsNumpy()
            hourly_temperature_2m = hourly.Variables(1).ValuesAsNumpy()

            hourly_data = {"date": pd.date_range(
            start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
            end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = hourly.Interval()),
            inclusive = "left"
            )}
            hourly_data["global_tilted_irradiance"] = hourly_global_tilted_irradiance
            hourly_data["temperature_2m"] = hourly_temperature_2m
            hourly_dataframe = pd.DataFrame(data = hourly_data)
            daily_dataframe= hourly_dataframe.resample('D', on='date').mean()
            return daily_dataframe

        elif self.energy_type == "eolienne":
            
            retry_session = retry(requests.Session(), retries=5, backoff_factor=0.2)
            openmeteo_eolienne = openmeteo_requests.Client(session=retry_session)
            params_eolienne = {
              "latitude": 43.6109,
              "longitude": 3.8763,
              "start_date": "2016-09-01",
              "end_date": "2025-09-29",
              "timezone": "UTC",
              "daily": ["temperature_2m_mean", "wind_speed_10m_mean", "pressure_msl_mean"],
            }
            responses = openmeteo_eolienne.weather_api(self.api_url, params=params_eolienne)
            daily = responses[0].Daily()
            daily_temperature_2m_mean = daily.Variables(0).ValuesAsNumpy()
            daily_wind_speed_10m_mean = daily.Variables(1).ValuesAsNumpy()
            daily_pressure_msl_mean = daily.Variables(2).ValuesAsNumpy()

            daily_data = {"date": pd.date_range(
            start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
            end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = daily.Interval()),
            inclusive = "left"
            )}

            daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
            daily_data["wind_speed_10m_mean"] = daily_wind_speed_10m_mean
            daily_data["pressure_msl_mean"] = daily_pressure_msl_mean

            daily_dataframe = pd.DataFrame(data = daily_data)
            return daily_dataframe


        elif self.energy_type == "hydro":
            all_data = []
            code_entite = "Y321002101"
            grandeurs = ["QmnJ", "HIXnJ"]

            for grandeur in grandeurs:
                params = {
                            "code_entite": code_entite, 
                            "grandeur_hydro_elab": grandeur, 
                            "date_debut_obs" : "2022-09-01",
                            "date_fin_obs": "2025-09-29",
                            "size": 1500,}
                response = requests.get(self.api_url, params=params)
                response.raise_for_status()
                df = pd.DataFrame(response.json().get("data", []))
                if not df.empty:
                    df["grandeur_hydro_elab"] = grandeur
                    all_data.append(df)
            if all_data:
                return pd.concat(all_data, ignore_index=True)
            return df

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.energy_type == "solaire":
          if "date" in df.columns:
            df = df.reset_index(drop=True)
          else:
            df = df.reset_index().rename(columns={"index": "date"})

          df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
          df = df.sort_values("date").drop_duplicates("date", keep="first")
          df = df.dropna(subset=["date"]).reset_index(drop=True)
          df["date"] = df["date"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
          return df
        
        elif self.energy_type == "eolienne":
          if "date" in df.columns:
            df = df.reset_index(drop=True)
          else:
            df = df.reset_index().rename(columns={"index": "date"})

          df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
          df = df.sort_values("date").drop_duplicates("date", keep="first")
          df = df.dropna(subset=["date"]).reset_index(drop=True)
          df["date"] = df["date"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
          return df
        
        if self.energy_type == "hydro":
          start="2022-09-01"
          end="2025-09-29"
          expected_cols=("QmnJ", "HIXnJ")

          if df.empty:
              print("Aucune donnée API")
              end = end or date.today().isoformat()
              idx = pd.date_range(start, end, freq="D")
              out = pd.DataFrame(index=idx).reset_index().rename(columns={"index": "date"})
              out.insert(0, "id", range(1, len(out) + 1))
              return out
          
          df = df.copy()
          df["date"] = pd.to_datetime(df["date_obs_elab"]).dt.normalize()
          df = df[["date", "grandeur_hydro_elab", "resultat_obs_elab"]]

          df_pivot = (
              df.pivot_table(
                  index="date",
                  columns="grandeur_hydro_elab",
                  values="resultat_obs_elab",
                  aggfunc="mean"
              )
              .sort_index()
          )

          end = end or date.today().isoformat()
          full_idx = pd.date_range(start, end, freq="D")
          df_pivot = df_pivot.reindex(full_idx)

          present_cols = [c for c in expected_cols if c in df_pivot.columns]
          if not present_cols:
              out = df_pivot.reset_index().rename(columns={"index": "date"})
              out.insert(0, "id", range(1, len(out) + 1))
              return out

          df_pivot = df_pivot[present_cols]
          for col in df_pivot.columns:
              df_pivot[col] = pd.to_numeric(df_pivot[col], errors="coerce")

          bounds_max = {
              "QmnJ": 10000,
              "HIXnJ": 2000 
          }
          for col in df_pivot.columns:
              df_pivot[col] = df_pivot[col].where(df_pivot[col] > 0)
              if col in bounds_max:
                  df_pivot[col] = df_pivot[col].where(df_pivot[col] < bounds_max[col])

          for col in df_pivot.columns:
              series = df_pivot[col].dropna()
              if series.empty:
                  continue
              Q1 = series.quantile(0.25)
              Q3 = series.quantile(0.75)
              IQR = Q3 - Q1
              low = Q1 - 1.5 * IQR
              high = Q3 + 1.5 * IQR
              df_pivot[col] = df_pivot[col].where((df_pivot[col] >= low) & (df_pivot[col] <= high))

          out = df_pivot.reset_index().rename(columns={"index": "date"})
          out["date"] = out["date"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
          out = out[["date","QmnJ", "HIXnJ"]].dropna()


cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://archive-api.open-meteo.com/v1/archive"
params = {
	"latitude": 43.6109,
	"longitude": 3.8763,
	"start_date": "2016-09-01",
	"end_date": "2025-09-25",
	"hourly": ["global_tilted_irradiance", "temperature_2m", "wind_speed_50m"],
	"tilt": 35,
}
responses = openmeteo.weather_api(url, params=params)
response = responses[0]
hourly = response.Hourly()
hourly_global_tilted_irradiance = hourly.Variables(0).ValuesAsNumpy()
hourly_temperature_2m = hourly.Variables(1).ValuesAsNumpy()
hourly_wind_speed_50m = hourly.Variables(2).ValuesAsNumpy()
