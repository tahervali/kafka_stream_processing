# weather.py
import requests


def get_temperature(city):
    print("****************TAHEER************")
    url = f"https://api.weather.com/{city}"
    response = requests.get(url)
    data = response.json()
    return data["temperature"]
