# ingest/meteostat_ingest.py

import os
import requests
import pandas as pd
import boto3
from dotenv import load_dotenv
from time import sleep

load_dotenv()

RAPIDAPI_KEY = os.getenv("METEOSTAT_API_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")

LAT = 37.7749
LON = -122.4194
START_YEAR = 1973
END_YEAR = 2022

session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
    region_name=os.getenv("AWS_REGION")
)
s3 = session.client("s3")

def get_daily_weather_by_year(lat, lon, year):
    url = "https://meteostat.p.rapidapi.com/point/daily"
    headers = {
        "x-rapidapi-host": "meteostat.p.rapidapi.com",
        "x-rapidapi-key": RAPIDAPI_KEY
    }
    start = f"{year}-01-01"
    end = f"{year}-12-31"
    params = {"lat": lat, "lon": lon, "start": start, "end": end}
    print(f"📡 Descargando clima {year}...")
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return pd.DataFrame(response.json()["data"])

def upload_to_s3(df, year):
    filename = f"sf_weather_{year}.csv"
    tmp_path = f"/tmp/{filename}"
    df.to_csv(tmp_path, index=False)
    s3_key = f"raw/weather/{filename}"
    s3.upload_file(tmp_path, S3_BUCKET, s3_key)
    print(f"✅ Subido {year} a s3://{S3_BUCKET}/{s3_key}")

if __name__ == "__main__":
    for year in range(START_YEAR, END_YEAR + 1):
        try:
            df = get_daily_weather_by_year(LAT, LON, year)
            upload_to_s3(df, year)
            sleep(1)  # evita bloqueos por demasiadas peticiones
        except Exception as e:
            print(f"❌ Error en {year}: {e}")
