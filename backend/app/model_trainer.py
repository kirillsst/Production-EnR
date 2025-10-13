import pandas as pd
import numpy as np
import joblib
from sklearn.model_selection import TimeSeriesSplit, RandomizedSearchCV, cross_val_score
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
from sklearn.ensemble import RandomForestRegressor
from pathlib import Path
from typing import List, Dict, Any

class ModelTrain:
    def __init__(self, producer_type: str,
                 features: List[str],
                 target: str,
                 save_dir: str = 'saved_models',
                 random_state: int = 5):
        
        self.producer_type = producer_type
        self.features = features
        self.target = target
        self.save_dir = Path(save_dir)
        self.random_state = random_state
        
        self.model = None
        self.metrics = {}
        
        self.save_dir.mkdir(parents=True, exist_ok=True)
        
    def train(self,
              data: pd.DataFrame,
              n_splits: int=5,
              n_iter_search: int=20,
              test_size: float=0.2) -> Dict[str, Any]:
        # Features et target
        X = data[self.features]
        y = data[self.target]
        # Découpage train/test temporel
        split_index = int(len(data) * (1 - test_size))
        X_train, X_test = X.iloc[:split_index], X.iloc[split_index:]
        y_train, y_test = y.iloc[:split_index], y.iloc[split_index:]
        # Définition du modèle
        base_model = RandomForestRegressor(random_state=self.random_state, n_jobs=-1)
        param_dist = {
            'n_estimators': [100, 200, 300],
            'max_depth': [5, 10, None],
            'min_samples_split': [2, 5, 10],
            'min_samples_leaf': [1,2,4]
        }
        # Optimisation avec RandomSearchCV et TimeSeriesSplit
        tscv = TimeSeriesSplit(n_splits=n_splits)
        search = RandomizedSearchCV(
            estimator=base_model,
            param_distributions=param_dist,
            n_iter=n_iter_search,
            scoring='r2',
            cv=tscv,
            random_state=self.random_state,
            n_jobs=-1,
            verbose=1
        )
        print(f"Lancement de l'optimisation des hyperparamètres pour {self.producer_type} (RandomForestRegressor)")
        search.fit(X_train, y_train)
        
        self.model = search.best_estimator_
        print(f" Optimisation terminée. Paramètrage optimale : {search.best_params_}")
        
        # Prédiction
        y_train_pred = self.model.predict(X_train)
        y_test_pred = self.model.predict(X_test)
        
        # Evaluation
        
        self.metrics = {
            "R2_train": r2_score(y_train, y_train_pred),
            "R2_test": r2_score(y_test, y_test_pred),
            "MAE": mean_absolute_error(y_test, y_test_pred),
            "MSE": mean_squared_error(y_test, y_test_pred),
            "RMSE": np.sqrt(mean_squared_error(y_test, y_test_pred)),
            "Best_params": search.best_params_
        }

        
        # Validation croisée temporelle
        
        cv_scores = cross_val_score(self.model, X, y, cv=tscv, scoring='r2', n_jobs=-1)
        self.metrics["R2_CV_mean"] = np.mean(cv_scores)
        self.metrics["R2_CV_std"] = np.std(cv_scores)
        
        # Sauvegarde du modèle
        print(Path(__file__).parent )
        model_path = Path(__file__).parent / self.save_dir / f"{self.producer_type}_random_forest_model.pkl"
        joblib.dump(self.model, model_path)
        print(f"Modèle sauvegardé ici : {model_path}")
        
        # Affichage résultats
        print("\n--- Résultats d'entraînement ---")
        print(f"Modèle entraîné pour {self.producer_type} : RandomForestRegressor")
        print(f"R² train : {self.metrics['R2_train']:.3f}")
        print(f"R² test  : {self.metrics['R2_test']:.3f}")
        print(f"R² CV    : {self.metrics['R2_CV_mean']:.3f} +/- {self.metrics['R2_CV_std']:.3f}")
        print(f"MAE      : {self.metrics['MAE']:.3f}")
        print(f"RMSE     : {self.metrics['RMSE']:.3f}")
        print(f"Meilleurs hyperparamètres : {self.metrics['Best_params']}")

        return self.metrics
    
    def predict(self, X_new: pd.DataFrame) -> np.ndarray:
        if self.model is None:
            raise ValueError("Le modèle n'a pas été entrainé ou chargé.")
        if not all(feature in X_new.columns for feature in self.features):
            raise ValueError(f"Colonnes d'entrées non correspondantes aux features attendues : {self.features}")
        X_new_ordered = X_new[self.features]
        return self.model.predict(X_new_ordered)
    
    @classmethod
    def load(cls, producer_type, features, target, save_dir="saved_models"):
        base_dir = Path(__file__).parent 
        model_path = base_dir / save_dir / f"{producer_type}_random_forest_model.pkl"

        if not model_path.exists():
            raise FileNotFoundError(f"Modèle non trouvé: {model_path}")

        instance = cls(producer_type=producer_type, features=features, target=target, save_dir=save_dir)
        instance.model = joblib.load(model_path)
        return instance
        
        
    