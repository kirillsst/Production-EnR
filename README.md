# Production_enr
Projet de prédiction de production d'énergie

## Objectif général

L’application repose sur :
- un **backend** en FastAPI (gestion des modèles et des prédictions)
- un **frontend** en Streamlit (interface utilisateur)
- une **base de données Supabase**

## Objectifs pédagogiques

- Manipuler et nettoyer des données issues d’APIs (Open-Meteo, Hub'eau)
- Créer des *features* pertinentes pour la modélisation (feature engineering)
- Entraîner des modèles de machine learning adaptés à chaque source d’énergie
- Construire une API REST performante avec **FastAPI**
- Développer une interface intuitive avec **Streamlit**
- Organiser un projet Python modulaire et maintenable

## Architecture du projet

──

```bash
production_enr/
|──backend/
|  |──app/
|     |──routes/
|        |──eolienne.py
|        |──hydro.py
|        |──solaire.py
|     |──main.py           # App FastAPI
|     |──model_trainer.py  # Classe d'entraînement des modèles
|     |──train_models.py   # Script principal d'entraînement
|  |──saved_models/        # Dossier de sauvegarde des modèles entrainés
|──frontend/
|  |──pages
|     |──Hydro.py
|     |──Solar.py
|     |──Wind.py
|  |──app.py               # Interface streamlit
|──handlers/
|  |──datahandler.py       # Classe de gestion des données 
|──notebooks/
|  |──notebook_prod_eolienne.ipynb
|  |──notebook_prod_hydro.ipynb
|  |──notebook_samy.ipynb  # Analyse des données de production solaire
|  |──notebook_select_model.ipynb
|
```

## Technologies utilisées

| Domaine          | Technologie                 |
| ---------------- | --------------------------- |
| Backend          | FastAPI                     |
| Frontend         | Streamlit                   |
| Machine Learning | scikit-learn, pandas, numpy |
| Base de données  | Supabase (PostgreSQL)       |
| APIs externes    | Open-Meteo, API Hydro       |
| Déploiement      | (optionnel) Docker, Uvicorn |

## Installation
### Cloner le projet
```
git clone https://github.com/GaetanCSimplon/production_enr.git

cd production_enr
```
## Créer et activer un environnement virtuel

```
python -m venv .venv
source .venv/bin/activate    # (Linux/Mac)
.venv\Scripts\activate       # (Windows)
```
## Installer les dépendances

```
uv sync
```

## Connexion à fastapi
```
cd backend/app/
uv run --active fastapi dev main.py
```

## Connexion à l'interface Streamlit

```
cd frontend/
streamlit run app.py
```

## Utilisation

1. Choisir le type d’énergie (éolienne, solaire, hydro)

2. Renseigner les variables météo :

- Éolien → wind_speed, temperature, pression

3. L’application calcule automatiquement :

- wind_speed3 = wind_speed ** 3

- temp_press = temperature * pression

4. L’API FastAPI envoie ces données au modèle entraîné

5. Le résultat (production prédite) est affiché dans Streamlit

## Modèle de prédiction

| Énergie  | Variables d’entrée                                                                             | Modèle utilisé   |
| -------- | ---------------------------------------------------------------------------------------------- | ---------------- |
| Éolienne | `wind_speed_10m_mean`, `pressure_msl_mean`, `temperature_2m_mean`, `wind_speed3`, `temp_press` | Random Forest    |
| Solaire  | `global_tilted_irradiance`, `temperature_2m`                                                   | Ridge Regression |
| Hydro    | `QmnJ`, `HIXnJ`                                                                                | Random Forest    |

## Entraîner les modèles

Pour réentrainer un modèle :

```
# Pour la production éolienne
python backend/app/train_model.py eolienne
# Pour la production solaire
python backend/app/train_model.py solaire
# Pour la production hydro-électrique
python backend/app/train_model.py hydro
```
- Le modèle est sauvegardé automatiquement après entraînement dans le dossier saved_models

## Améliorations possibles

- Automatiser la mise à jour quotidiennes des données API
- Implémenter un pipeline de monitoring
- Déploiement

## Crédits

Kirill - https://github.com/kirillsst

Samy - https://github.com/samyachd

Gaëtan - https://github.com/GaetanCSimplon