# ingest/meteostat_ingest.py

import os
import requests
import pandas as pd
import boto3
from dotenv import load_dotenv

load_dotenv()

RAPIDAPI_KEY = os.getenv("METEOSTAT_API_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")

def get_monthly_weather(lat, lon, start, end):
    url = "https://meteostat.p.rapidapi.com/point/monthly"
    headers = {
        "x-rapidapi-host": "meteostat.p.rapidapi.com",
        "x-rapidapi-key": RAPIDAPI_KEY
    }
    params = {
        "lat": lat,
        "lon": lon,
        "start": start,
        "end": end
    }
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return pd.DataFrame(response.json()["data"])

def upload_to_s3(df, filename, s3_key):
    tmp_path = f"/tmp/{filename}"
    df.to_csv(tmp_path, index=False)
    s3 = boto3.client("s3")
    s3.upload_file(tmp_path, S3_BUCKET, s3_key)
    print(f"✅ Subido a S3: s3://{S3_BUCKET}/{s3_key}")

if __name__ == "__main__":
    latitude = 37.7749
    longitude = -122.4194
    start_date = "2022-01-01"
    end_date = "2022-12-31"

    df_weather = get_monthly_weather(latitude, longitude, start_date, end_date)
    upload_to_s3(df_weather, "sf_weather_2022.csv", "raw/weather/sf_weather_2022.csv")
