import os
import pandas as pd
import folium

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "../data/live_global_env_data.csv")
OUTPUT_PATH = os.path.join(BASE_DIR, "../output/global_env_map.html")



df = pd.read_csv(DATA_PATH)


numeric_cols = ['Latitude', 'Longitude', 'Temperature', 'Humidity', 'CO', 'PM2.5']
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')

df = df.dropna(subset=['Latitude', 'Longitude'])


m = folium.Map(location=[20, 0], zoom_start=2)


for _, row in df.iterrows():
    pm = row['PM2.5'] if not pd.isna(row['PM2.5']) else 0

    # Color selection
    if pm < 12:
        color = "green"
    elif pm < 35:
        color = "orange"
    else:
        color = "red"

    radius = 5 + pm * 0.5

    # HTML popup with safety link
    popup_html = f"""
    <b>{row['City']}, {row['Country']}</b><br>
    Temp: {row['Temperature']}°C<br>
    Humidity: {row['Humidity']}%<br>
    PM2.5: {row['PM2.5']} µg/m³<br>
    CO: {row['CO']} µg/m³<br>
    Timestamp: {row['Timestamp']}<br><br>

    <a href="safety.html?color={color}" target="_blank"
       style="padding:6px 10px; background:{color}; color:white; 
              text-decoration:none; border-radius:6px;">
       Safety Instructions
    </a>
    """

    folium.CircleMarker(
        location=[row['Latitude'], row['Longitude']],
        radius=radius,
        popup=folium.Popup(popup_html, max_width=300),
        color=color,
        fill=True,
        fill_color=color,
        fill_opacity=0.7
    ).add_to(m)


os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)


m.save(OUTPUT_PATH)
print(f" Map saved at: {OUTPUT_PATH}")
