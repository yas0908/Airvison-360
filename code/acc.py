
import os
import pandas as pd
import joblib
from azure.storage.blob import BlobServiceClient

from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error
from sklearn.ensemble import StackingRegressor

from xgboost import XGBRegressor
from lightgbm import LGBMRegressor
from sklearn.linear_model import Ridge


CONN_STR = "DefaultEndpointsProtocol=https;AccountName=globalenvdata;AccountKey=C6HgpHuHbtsBJDRNllKXFpGydTyX+lescDvNw7Hle2/Zf9VfoQvDZrBHQ9IMxaj9AD4/Et/P8D/d+ASteJqVMA==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "globalenvdata"

LIVE_BLOB = "live_global_env_data.csv"
HIST_BLOB = "historical_global_env_data.csv"

DATA_DIR = "data"
MODEL_DIR = "models"
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(MODEL_DIR, exist_ok=True)

LIVE_PATH = os.path.join(DATA_DIR, LIVE_BLOB)
HIST_PATH = os.path.join(DATA_DIR, HIST_BLOB)
FULL_PATH = os.path.join(DATA_DIR, "full_env_data.csv")
MODEL_PATH = os.path.join(MODEL_DIR, "stacked_env_model.pkl")

print("🔗 Connecting to Azure Blob Storage...")
blob_service_client = BlobServiceClient.from_connection_string(CONN_STR)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

def download_blob(blob_name, local_path):
    try:
        blob_client = container_client.get_blob_client(blob_name)
        with open(local_path, "wb") as f:
            f.write(blob_client.download_blob().readall())
        print(f" Downloaded '{blob_name}' → '{local_path}'")
        return True
    except Exception as e:
        print(f" Could not download {blob_name}: {e}")
        return False

has_live = download_blob(LIVE_BLOB, LIVE_PATH)
has_hist = download_blob(HIST_BLOB, HIST_PATH)

dfs = []
if has_hist:
    dfs.append(pd.read_csv(HIST_PATH))
if has_live:
    dfs.append(pd.read_csv(LIVE_PATH))

if not dfs:
    raise FileNotFoundError(" No data found in Blob Storage. Please upload live/historical data first.")

df = pd.concat(dfs, ignore_index=True).drop_duplicates(subset=["City", "Timestamp"], keep="last")
df.to_csv(FULL_PATH, index=False)
print(f" Combined dataset saved → {FULL_PATH}")
print(f"Total records: {len(df)}")

df = df.dropna(subset=["PM2.5", "Temperature", "Humidity", "CO", "Timestamp"])
df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")

df["Hour"] = df["Timestamp"].dt.hour
df["Day"] = df["Timestamp"].dt.day
df["Month"] = df["Timestamp"].dt.month

df["Temp_Humidity_Interaction"] = df["Temperature"] * df["Humidity"]
df["CO_Temp_Interaction"] = df["CO"] * df["Temperature"]


feature_cols = [
    "Temperature", "Humidity", "CO", "City", "Hour", "Day", "Month",
    "Temp_Humidity_Interaction", "CO_Temp_Interaction"
]
X = df[feature_cols]
y = df["PM2.5"]

categorical_features = ["City"]
numeric_features = [col for col in feature_cols if col not in categorical_features]

preprocessor = ColumnTransformer(
    transformers=[
        ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_features),
        ("num", StandardScaler(), numeric_features)
    ]
)


xgb = XGBRegressor(
    random_state=42,
    objective="reg:squarederror",
    n_estimators=300,
    learning_rate=0.05,
    max_depth=6,
    subsample=0.8,
    colsample_bytree=0.8
)

lgbm = LGBMRegressor(
    random_state=42,
    n_estimators=300,
    learning_rate=0.05,
    max_depth=6,
    subsample=0.8,
    colsample_bytree=0.8
)


stacked_model = StackingRegressor(
    estimators=[("xgb", xgb), ("lgbm", lgbm)],
    final_estimator=Ridge(alpha=1.0)
)

pipeline = Pipeline([
    ("preprocessor", preprocessor),
    ("regressor", stacked_model)
])


X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)


print("🚀 Training high-accuracy stacked model (XGBoost + LightGBM)...")
pipeline.fit(X_train, y_train)

y_pred = pipeline.predict(X_test)
r2 = r2_score(y_test, y_pred)
mse = mean_squared_error(y_test, y_pred)
mae = mean_absolute_error(y_test, y_pred)
accuracy = r2 * 100

print("\nMODEL PERFORMANCE")
print(f"R² Score: {r2:.4f}")
print(f"Accuracy: {accuracy:.2f}%")
print(f"MSE: {mse:.3f}")
print(f"MAE: {mae:.3f}")


joblib.dump(pipeline, MODEL_PATH)
print(f"Model saved → {MODEL_PATH}")


try:
    blob_client = container_client.get_blob_client("models/high_accuracy_env_model.pkl")
    with open(MODEL_PATH, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)
    print(" Uploaded trained model back to Azure Blob Storage.")
except Exception as e:
    print(f" Could not upload model to Blob: {e}") 