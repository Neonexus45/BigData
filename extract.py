import requests
import pandas as pd

def get_all_data(link, total_size):
    data = []
    for i in range(0, total_size, 10):
        request = requests.get(f"{link}?limit=100&offset={i}")
        data += request.json().get("results", [])
    return data

def pre_process(link):
    request = requests.get(f"{link}?limit=1")
    data = request.json()
    total_size = data.get("total_count", 0)
    return get_all_data(link, total_size)

def save_as_csv(data, filename):
    if data:
        df = pd.DataFrame(data)  
        df.to_csv(filename, index=False)
        print(f"Data saved as {filename}")
    else:
        print("No valid records found to save.")

def __main__():
    link = ("https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/sites-disposant-du-service-paris-wi-fi"
            "/records")
    data = pre_process(link)

    save_as_csv(data, r"C:\Users\dell\Downloads\paris_wifi_sites.csv")

__main__()
