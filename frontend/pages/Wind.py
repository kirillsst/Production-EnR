import streamlit as st
import requests
import pandas as pd

st.set_page_config(page_title="Production Eolienne", page_icon="⚡", layout="wide")
st.title("Simulation Eolienne")

wind_speed_10m_mean = st.number_input("Wind speed 10m mean", min_value=0.0, max_value=5000.0, value=1000.0)
pressure_msl_mean = st.number_input("Pressure msl mean", min_value=0.0, max_value=2000.0, value=400.0)
temperature_2m_mean = st.number_input("Temperature 2m mean", min_value=0.0, max_value=2000.0, value=400.0)

if st.button("Prédire"):
    try:
        response = requests.post(
            "http://localhost:8000/predict/eolienne",
            json={"wind_speed_10m_mean": wind_speed_10m_mean, "pressure_msl_mean": pressure_msl_mean, "temperature_2m_mean": temperature_2m_mean }
        )

        if response.status_code == 200:
            data = response.json()
            if "prediction" in data:
                pred = data["prediction"]
                st.success(f"Production eolienne prédite : **{pred:.2f} kW**")
            elif "error" in data:
                st.warning(f"{data['error']}")
            else:
                st.error("Réponse inattendue du serveur.")
        else:
            data = response.json()
            st.error(f"Erreur de prédiction : {data.get('error', 'Erreur inconnue')}")

    except requests.exceptions.ConnectionError:
        st.error("Impossible de se connecter au serveur FastAPI. Vérifie s’il est bien lancé.")
    except Exception as e:
        st.error(f"Une erreur est survenue : {str(e)}")

