from datetime import datetime, timezone
import json

class TransformWeatherData:
    def __init__(self):
        pass

    def transform(self, city_data):
        """ This is what a typical city weather data looks like.
            {'coord': {'lon': 7.4896, 'lat': 5.5263},
            'weather': [{'id': 804,
                'main': 'Clouds',
                'description': 'overcast clouds',
                'icon': '04n'}],
            'base': 'stations',
            'main': {'temp': 21.34,
                'feels_like': 22.11,
                'temp_min': 21.34,
                'temp_max': 21.34,
                'pressure': 1015,
                'humidity': 99,
                'sea_level': 1015,
                'grnd_level': 997},
            'visibility': 10000,
            'wind': {'speed': 0.51, 'deg': 291, 'gust': 0.56},
            'clouds': {'all': 98},
            'dt': 1758760667,
            'sys': {'country': 'NG', 'sunrise': 1758777523, 'sunset': 1758821081},
            'timezone': 3600,
            'id': 2320576,
            'name': 'Umuahia',
            'cod': 200} 
        """
        try:
            #SELECT KEY INFORMATION
            transformed_record = {
                "city_id": city_data['id'],
                "city_name": city_data['name'],
                "country_code": city_data['sys']['country'],
                "temperature_celsius": city_data['main']['temp'],
                "feels_like_celsius": city_data['main']['feels_like'],
                "min_temperature_celsius": city_data['main']['temp_min'],
                "max_temperature_celsius": city_data['main']['temp_max'],
                "humidity_percent": city_data['main']['humidity'],
                "pressure_hpa": city_data['main']['pressure'],
                "visibility_meters": city_data.get('visibility'), # .get() for safety
                "wind_speed_ms": city_data['wind']['speed'],
                "wind_direction_deg": city_data['wind']['deg'],
                "weather_condition": city_data['weather'][0]['main'],
                "weather_description": city_data['weather'][0]['description'],
            }
            # Get unix timestamps from the raw data & convert them to datetime objects
            # enrichment
            observation_dt = datetime.fromtimestamp(city_data['dt'], tz=timezone.utc)
            sunrise_dt = datetime.fromtimestamp(city_data['sys']['sunrise'], tz=timezone.utc)
            sunset_dt = datetime.fromtimestamp(city_data['sys']['sunset'], tz=timezone.utc)

            # Format them into readable strings
            transformed_record["observation_timestamp_utc"] = observation_dt.strftime('%Y-%m-%d %H:%M:%S')
            transformed_record["sunrise_utc"] = sunrise_dt.strftime('%Y-%m-%d %H:%M:%S')
            transformed_record["sunset_utc"] = sunset_dt.strftime('%Y-%m-%d %H:%M:%S')

        except (KeyError, IndexError, TypeError) as e:
            # If any expected key is missing or the data format is wrong, skip this record.
            city_name = city_data.get('name', 'Unknown City')
            print(f"Warning: Could not transform data for '{city_name}'. Error: {e}. Skipping record.")
            return None
        
    def run_transform(self, weather_data):
        transformed_weather_data = []
        
        for city_data in weather_data:
            transformed_city = self.transform(city_data)
            if transformed_city:  # Only append if the transformation was successful
                transformed_weather_data.append(transformed_city)

        print(f"Transformation complete. Successfully transformed {len(transformed_weather_data)} records.")
        return transformed_weather_data

