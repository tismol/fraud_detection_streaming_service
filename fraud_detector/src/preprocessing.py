import numpy as np
import pandas as pd


def haversine_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    R = 6371.0
    return R * c


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    dt = pd.to_datetime(df["transaction_time"])
    df["hour"] = dt.dt.hour
    df["dow"] = dt.dt.dayofweek
    df["month"] = dt.dt.month
    df["is_weekend"] = (df["dow"] >= 5).astype(int)
    df["is_night"] = ((df["hour"] >= 22) | (df["hour"] <= 6)).astype(int)
    return df


def add_geo_features(df: pd.DataFrame) -> pd.DataFrame:
    df["distance_km"] = haversine_km(df["lat"], df["lon"], df["merchant_lat"], df["merchant_lon"])
    df["lat_diff"] = (df["lat"] - df["merchant_lat"]).abs()
    df["lon_diff"] = (df["lon"] - df["merchant_lon"]).abs()
    df["is_local"] = (df["distance_km"] <= 5).astype(int)
    df["is_far"] = (df["distance_km"] >= 500).astype(int)
    return df


def preprocess(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str], list[str]]:
    df["customer_id"] = df["name_1"].astype(str) + "_" + df["name_2"].astype(str) + "_" + df["post_code"].astype(str)
    df = add_time_features(df)
    df = add_geo_features(df)
    df["amount_log"] = np.log1p(df["amount"])
    cat_cols = ["merch", "cat_id", "name_1", "name_2", "gender", "street",
                "one_city", "us_state", "post_code", "jobs", "customer_id"]

    for c in cat_cols:
        df[c] = df[c].astype(str)

    features = [c for c in df.columns if c not in ["target", "transaction_time"]]

    return df, features, cat_cols
