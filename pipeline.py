
import pandas as pd
import logging
import time
import requests
from sqlalchemy import create_engine 
CITIES = [
    {'city': 'Mumbai', 'lat': 19.07, 'lon': 72.87},
    {'city': 'Delhi', 'lat': 28.61, 'lon': 77.20},
    {'city': 'Bangalore', 'lat': 12.97, 'lon': 77.59},
    {'city': 'Chennai', 'lat': 13.08, 'lon': 80.27},
    {'city': 'Kolkata', 'lat': 22.57, 'lon': 88.36},
    {'city': 'Hyderabad', 'lat': 17.38, 'lon': 78.47},
    {'city': 'Pune', 'lat': 18.52, 'lon': 73.85},
    {'city': 'Ahmedabad', 'lat': 23.02, 'lon': 72.57},
    {'city': 'Jaipur', 'lat': 26.91, 'lon': 75.79},
    {'city': 'Lucknow', 'lat': 26.85, 'lon': 80.95}
]

logging.basicConfig(
    filename='weather_pipeline1.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def extract(CITIES):
    try:
        results = []
        for city in CITIES:
            url = f"https://api.open-meteo.com/v1/forecast?latitude={city['lat']}&longitude={city['lon']}&current_weather=true"
            response = requests.get(url)
            data = response.json()
            weather = data['current_weather']
            weather['city'] = city['city']
            results.append(weather)
        
        df = pd.DataFrame(results)
        logging.info(f"Extracted weather data for {len(df)} cities")
        return df
    
    except Exception as e:
        logging.error(f"Extract failed: {e}")
        raise
def assign_heat_category(temp):
        if temp < 15:
         return 'Cold'
        elif temp < 25:
          return 'Moderate'
        elif temp < 35:
          return 'Hot'
        else:
          return 'Very Hot'


def transform(df):
    try:
        # Add heat category
        df['heat_category'] = df['temperature'].apply(assign_heat_category)
        
        # Add weather description based on weathercode
        df['weather_description'] = df['weathercode'].apply(lambda x: 
            'Clear sky' if x == 0 else
            'Partly cloudy' if x in [1, 2, 3] else
            'Foggy' if x in [45, 48] else
            'Rainy' if x in [51, 53, 55, 61, 63, 65] else
            'Thunderstorm' if x in [95, 96, 99] else
            'Other'
        )
        
        # Drop columns we don't need
        df = df.drop(columns=['interval', 'weathercode'])
        
        logging.info(f"Transform successful - {len(df)} rows, {len(df.columns)} columns")
        return df
    
    except Exception as e:
        logging.error(f"Transform failed: {e}")
        raise
def load(df, engine):
    try:
       
        df.to_sql('weather_data', engine, if_exists='replace', index=False)
        logging.info(f"Successfully loaded {len(df)} rows into weather_data table")
    
    except Exception as e:
        logging.error(f"Load failed: {e}")
        raise
def validate(df):
    errors = []
    
    if df['temperature'].isnull().any():
        errors.append("Missing values found in temperature column")
    
    if ((df['temperature'] < -10) | (df['temperature'] > 50)).any():
        errors.append("Temperature values out of realistic range")
    
    if df['city'].duplicated().any():
        errors.append("Duplicate  city's found")
    
    if errors:
        for error in errors:
            logging.error(f"VALIDATION ERROR: {error}")
        return False
    
    logging.info("All validations passed!")
    return True

def run_pipeline():
    start = time.time()
    engine = create_engine("postgresql+psycopg2://postgres:admin123@localhost:5432/subiya")
    logging.info("Pipeline started")
    df = extract(CITIES)
    df = transform(df)
    if not validate(df):
        logging.error("Validation failed - pipeline stopped")
        return
    load(df, engine)
    duration = round(time.time() - start, 2)
    logging.info(f"Pipeline completed successfully in {duration} seconds")


run_pipeline()