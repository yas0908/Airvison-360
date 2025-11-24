import os
import pandas as pd
import requests
import time
from datetime import datetime


OPENWEATHER_KEY = "your openweather API key"



BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "../data")
os.makedirs(DATA_DIR, exist_ok=True)  # ensure folder exists

CITIES_PATH = os.path.join(DATA_DIR, "cities.csv")
LIVE_OUTPUT_PATH = os.path.join(DATA_DIR, "live_global_env_data.csv")
HISTORICAL_OUTPUT_PATH = os.path.join(DATA_DIR, "historical_global_env_data.csv")


cities_df = pd.read_csv(CITIES_PATH)
print(f" Loaded {len(cities_df)} cities!")


data = []


for idx, row in cities_df.iterrows():
    city = row['City']
    lat, lon = row['Latitude'], row['Longitude']
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"Fetching data for {city} ({idx+1}/{len(cities_df)})...")

    # Weather
    temperature, humidity = None, None
    try:
        weather_url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={OPENWEATHER_KEY}&units=metric"
        weather_resp = requests.get(weather_url, timeout=10).json()
        temperature = weather_resp.get('main', {}).get('temp')
        humidity = weather_resp.get('main', {}).get('humidity')
    except Exception as e:
        print(f" Weather API error for {city}: {e}")

    # Air Pollution
    co, pm25 = None, None
    try:
        pollution_url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={OPENWEATHER_KEY}"
        pollution_resp = requests.get(pollution_url, timeout=10).json()
        components = pollution_resp.get('list', [{}])[0].get('components', {})
        co = components.get('co')
        pm25 = components.get('pm2_5')
    except Exception as e:
        print(f" Pollution API error for {city}: {e}")

    data.append({
        "Timestamp": timestamp,
        "City": city,
        "Country": row['Country'],
        "Latitude": lat,
        "Longitude": lon,
        "Temperature": temperature,
        "Humidity": humidity,
        "CO": co,
        "PM2.5": pm25
    })

    time.sleep(1) 


df = pd.DataFrame(data)


df.to_csv(LIVE_OUTPUT_PATH, index=False)
print(f" Live data saved at: {LIVE_OUTPUT_PATH}")


if os.path.exists(HISTORICAL_OUTPUT_PATH):
    df.to_csv(HISTORICAL_OUTPUT_PATH, mode='a', header=False, index=False)
else:
    df.to_csv(HISTORICAL_OUTPUT_PATH, index=False)
print(f" Historical data updated at: {HISTORICAL_OUTPUT_PATH}")
