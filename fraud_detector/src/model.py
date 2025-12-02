import os

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier, Pool

from .preprocessing import preprocess

MODEL_PATH = os.getenv("MODEL_PATH", "/app/models/catboost_fraud.cbm")
THRESHOLD = float(os.getenv("FRAUD_THRESHOLD", "0.4"))


def load_model() -> CatBoostClassifier:
    model = CatBoostClassifier()
    model.load_model(MODEL_PATH)
    return model


def score(df: pd.DataFrame, model: CatBoostClassifier) -> pd.DataFrame:
    df_proc, features, cat_cols = preprocess(df.copy())
    pool = Pool(df_proc[features], cat_features=cat_cols)
    proba = model.predict_proba(pool)[:, 1]
    fraud_flag = (proba >= THRESHOLD).astype(int)
    return pd.DataFrame(
        {
            "score": proba.astype(float),
            "fraud_flag": fraud_flag.astype(int),
        }
    )
