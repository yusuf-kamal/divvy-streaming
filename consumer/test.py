import requests

url = "https://api.citybik.es/v2/networks/divvy"

response = requests.get(url, timeout=30)
response.raise_for_status()

data = response.json()
stations = data["network"]["stations"]

print("Stations in API:", len(stations))

