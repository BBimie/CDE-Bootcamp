import os
from datetime import date
from dotenv import load_dotenv
import os
import requests
import json
import gzip

load_dotenv()


class ExtractWeatherData:
    """
    A class to extract weather data for Nigerian capital cities
    from the OpenWeatherMap API.
    """
    def __init__(self, date=False):
        self.api_key = os.environ["API_KEY"]
        self.base_url = "https://api.openweathermap.org/data/2.5/weather"
        self.city_list_url = "http://bulk.openweathermap.org/sample/city.list.json.gz"
        self.city_list_path_gz = "docker_etl_pipeline/utils/city_list.json.gz"
        self.city_list_path_json = "docker_etl_pipeline/utils/city_list.json"
        self.date = date

    def _get_capital_cities(self) -> list:
        return [
            "Umuahia", "Yola", "Uyo", "Awka", "Bauchi", "Yenagoa", "Makurdi",
            "Maiduguri", "Calabar", "Asaba", "Abakaliki", "Benin", "Ado-Ekiti",
            "Enugu", "Gombe", "Owerri", "Dutse", "Kaduna", "Kano", "Katsina",
            "Birnin Kebbi", "Lokoja", "Ilorin", "Ikeja", "Lafia", "Minna",
            "Abeokuta", "Akure", "Oshogbo", "Ibadan", "Jos", "Port Harcourt",
            "Sokoto", "Jalingo", "Damaturu", "Gusau", "Abuja"
        ]

    def extract(self) -> list:
        capital_names = self._get_capital_cities()
        print(f"Fetching 5-day weather forecast for {len(capital_names)} Nigerian capital cities...")

        weather_data = []
        for name in capital_names:
            city_query = f"{name},NG"

            params = {
                'q': city_query,
                'appid': self.api_key,
                'units': 'metric'  # Get temperature in Celsius
            }

            try:
                response = requests.get(self.base_url, params=params)
                response.raise_for_status()
                
                # The entire JSON forecast data for the city is appended.
                weather_data.append(response.json())
                print(f"Successfully fetched data for: {name}")

            except requests.exceptions.RequestException as e:
                print(f"Could not fetch weather data for {name}. Error: {e}")

        if weather_data:
            print(f"\nSuccessfully extracted data for {len(weather_data)} cities.")
        else:
            print("\nExtraction finished, but no data was fetched. Please check the errors above.")

        print(weather_data)

        return weather_data

ExtractWeatherData().extract()
