
# Weather ETL Pipeline

A real-time ETL pipeline that extracts live weather data from an API,
transforms it and loads it into a PostgreSQL database.

## What it does
- Extracts live weather data for 10 Indian cities using Open Meteo API
- Transforms data — adds heat category and weather description
- Validates data quality before loading
- Loads clean data into PostgreSQL database
- Logs every step with timestamps

## Pipeline Flow
API → Extract → Transform → Validate → Load → PostgreSQL

## Key Insights
- Hyderabad was the hottest city at 35.4°C
- Lucknow was the only city with clear sky
- All cities validated successfully before loading

## Tools Used
- Python
- Pandas
- SQLAlchemy
- PostgreSQL
- psycopg2
- Open Meteo API (free, no authentication required)

## How to run
1. Install requirements: `pip install pandas sqlalchemy psycopg2-binary requests`
2. Update database connection string in `pipeline.py`
3. Run: `python pipeline.py`
4. Check `weather_pipeline.log` for execution details
