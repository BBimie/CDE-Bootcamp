import requests
import pandas as pd
import sqlite3
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ["API_KEY"]
URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"


class ExtractWeatherData:
    def __init__(self):
        pass

    def _down_city_json(self):
        pass

    def capital_cities(self) -> list:
        cities = [ "Umuahia", "Yola", "Uyo", "Awka", "Bauchi", "Yenagoa", "Makurdi", "Maiduguri", "Calabar", "Asaba", "Abakaliki", "Benin City", "Ado Ekiti", "Enugu", "Abuja", "Gombe", "Owerri", "Dutse", "Kaduna", "Kano", "Katsina", "Birnin Kebbi", "Lokoja", "Ilorin", "Ikeja", "Lafia", "Minna", "Abeokuta", "Akure", "Oshogbo", "Ibadan", "Jos", "Port Harcourt", "Sokoto", "Jalingo", "Damaturu", "Gusau", "Abuja"]
        return cities

def extract():
    response = requests.get(URL)
    response.raise_for_status()
    return response.json()

def transform(data):
    record = {
        "city": data["name"],
        "temperature": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "wind_speed": data["wind"]["speed"],
        "weather": data["weather"][0]["description"],
        "timestamp": datetime.utcfromtimestamp(data["dt"])
    }
    return record

def load(record):
    conn = sqlite3.connect("weather.db")
    df = pd.DataFrame([record])
    df.to_sql("weather", conn, if_exists="append", index=False)
    conn.close()

if __name__ == "__main__":
    raw = extract()
    cleaned = transform(raw)
    load(cleaned)
    print("ETL complete:", cleaned)
