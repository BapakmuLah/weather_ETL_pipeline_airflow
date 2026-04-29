import time
import json
import requests
import pandas as pd
import os

API_KEY = os.getenv('API_KEY')

# RATE LIMITER API
class RateLimiter:
    def __init__(self, max_calls, period):
        self.max_calls = max_calls
        self.period = period
        self.calls = []
    
    def wait(self):
        now = time.time()
        self.calls = [call for call in self.calls if call > now - self.period]
        if len(self.calls) >= self.max_calls:
            sleep_time = self.calls[0] + self.period - now
            time.sleep(sleep_time)
        self.calls.append(time.time())

def extract_data(output_path, **context):

    # ENTER CITY YG MAU DIAMBIL DARI API (DEFAULT 'JAKARTA')
    cities = context['params'].get('cities_input', ['Jakarta'])
    
    limiter = RateLimiter(max_calls=250, period=60)
    extracted_data = []

    for city in cities:
        limiter.wait()
        url = 'https://api.openweathermap.org/data/2.5/weather'
        params = {'q': f'{city},ID', 'units': 'metric', 'appid': API_KEY}
        
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            extracted_data.append({
                'city_id': data.get('id'),
                'city': data.get('name'),
                'datetime': data.get("dt"),
                'timezone': data.get("timezone"),
                'longitude': data.get("coord", {}).get("lon"),
                'latitude': data.get("coord", {}).get("lat"),
                'country': data.get('sys', {}).get('country'),
                'weather': (data.get('weather') or [{}])[0].get('main'),
                'weather_description' : (data.get('weather') or [{}])[0].get("description"),
                'weather_icon' : (data.get("weather") or [{}])[0].get("icon"),
                'temp': data.get('main', {}).get('temp'),
                'feels_like': data.get('main', {}).get('feels_like'),
                'temp_min': data.get('main', {}).get('temp_min'),
                'temp_max': data.get('main', {}).get('temp_max'),
                'pressure': data.get('main', {}).get('pressure'),
                'humidity': data.get('main', {}).get('humidity'),
                'sea_level': data.get('main', {}).get('sea_level'),
                'grnd_level': data.get('main', {}).get('grnd_level'),
                'wind_speed': data.get('wind', {}).get('speed'),
                'wind_degree': data.get('wind', {}).get('deg'),
                'sunrise': data.get('sys', {}).get('sunrise'),
                'sunset': data.get('sys', {}).get('sunset'),
                'rain': data.get('rain', {}).get('1h', 0),
                'cloudy': data.get('clouds', {}).get('all'),
                'base': data.get("base"),
                'visibility': data.get("visibility")
            })
        else:
            print(f"Failed to fetch {city}: Status {response.status_code}")

    # SAVE EXTRACTED DATA INTO JSON FILE 
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(extracted_data, f)
    
    print(f"Extraction complete. Data saved to {output_path}")