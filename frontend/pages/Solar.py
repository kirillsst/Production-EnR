import streamlit as st
import requests
import pandas as pd

st.set_page_config(page_title="Production Solaire", page_icon="⚡", layout="wide")
st.title("Simulation Solaire")

global_tilted_irradiance = st.number_input("Global tilted irradiance", min_value=0.0, max_value=5000.0, value=1000.0)
temperature_2m = st.number_input("Temperature 2m", min_value=0.0, max_value=2000.0, value=400.0)

if st.button("Prédire"):
    try:
        response = requests.post(
            "http://localhost:8000/predict/solaire",
            json={"global_tilted_irradiance": global_tilted_irradiance, "temperature_2m": temperature_2m}
        )

        if response.status_code == 200:
            data = response.json()
            if "prediction" in data:
                pred = data["prediction"]
                st.success(f"Production solaire prédite : **{pred:.2f} kW**")
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

