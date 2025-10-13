import streamlit as st
import requests
import pandas as pd

st.set_page_config(page_title="Production Hydro", page_icon="⚡", layout="wide")
st.title("Simulation Hydroélectrique")

qmnj = st.number_input("QmnJ (débit moyen journalier, en m³/s — quantité moyenne d’eau écoulée par jour)", min_value=0.0, max_value=5000.0, value=1000.0, help="Représente le débit moyen de la rivière sur une journée, en mètres cubes par seconde.")
hixnj = st.number_input("HIXnJ (indice hydrométrique journalier, mm — hauteur normalisée de l’eau)", min_value=0.0, max_value=2000.0, value=400.0,  help="Reflète la hauteur d’eau moyenne du jour, exprimée en millimètres, après normalisation.")

if st.button("Prédire"):
    try:
        response = requests.post(
            "http://localhost:8000/predict/hydro",
            json={"QmnJ": qmnj, "HIXnJ": hixnj}
        )

        if response.status_code == 200:
            data = response.json()
            if "prediction" in data:
                pred = data["prediction"]
                st.success(f"Production hydroélectrique prédite : **{pred:.2f} kW**")
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

