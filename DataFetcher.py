import requests
from datetime import datetime
from datetime import timedelta
import schedule
import time
import csv
import hashlib

MAPKEY = "35ff28ee263d53bed16d48c5c1ac851f"
current_date = '2024-05-28'
days_to_increment = 9

def is_item_unique(item, item_array):
    for existing_item in item_array:
        if item == existing_item:
            return False
    return True

def increment_date(current_date_str, days_to_increment):
    current_date = datetime.strptime(current_date_str, '%Y-%m-%d')
    new_date = current_date + timedelta(days=days_to_increment)
    new_date_str = new_date.strftime('%Y-%m-%d')
    return new_date_str

def fetch_and_save_data(url, prefix):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.5615.50 Safari/537.36"
    }
    print(f"Fetching {prefix} data.")
    response = requests.get(url, headers=headers)
    fname = f"data.csv"
    counter = 0
    with open(fname, 'a') as f:
        if response.text.startswith('country_id,latitude,longitude'):
            result_array = response.text.split('\n')
            print(f"{prefix}: {len(result_array)-1} rows found.")
            existing_array = "";
            with open(fname, 'r') as fr:
                existing_array = fr.read().split('\n')
            for item in result_array:
                if item.startswith('country_id,latitude,longitude'):
                    if existing_array == ['']:
                        is_unique = True
                    else:
                        is_unique = False
                elif item == "":
                    is_unique = False
                else:
                    is_unique = is_item_unique(f"{item}, {prefix}", existing_array)
                if is_unique:
                    if item.startswith('country_id,latitude,longitude'):
                        f.write(item  + "\n")
                        print(f"New {prefix} row: the row is unique.")
                    else:
                        f.write(f"{item}, {prefix}\n")
                        counter += 1
                        print(f"New {prefix} row: the row is unique.")
                else:
                    print(f"A {prefix} row is not unique, dropping...")
        else:
            if prefix == "noaa20":
                fetch_noaa20_data()
            elif prefix == "snpp":
                fetch_snpp_data()
            elif prefix == "modis":
                fetch_modis_data()
            elif prefix == "noaa21":
                fetch_noaa21_data()
            elif prefix == "snpp_sp":
                fetch_snpp_sp_data()
            elif prefix == "modis_sp":
                fetch_modis_sp_data()    
    print(f"{prefix}: data fetch finished, {counter} unique rows found.")

def fetch_noaa20_data():
    noaa20_url = f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAPKEY}/VIIRS_NOAA20_NRT/SDN/10/{current_date}"
    fetch_and_save_data(noaa20_url, "noaa20")

def fetch_noaa21_data():
    noaa21_url = f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAPKEY}/VIIRS_NOAA21_NRT/SDN/10/{current_date}"
    fetch_and_save_data(noaa21_url, "noaa21")

def fetch_snpp_sp_data():
    snpp_sp_url = f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAPKEY}/VIIRS_SNPP_SP/SDN/10/{current_date}"
    fetch_and_save_data(snpp_sp_url, "snpp_sp")
    
def fetch_snpp_data():
    snpp_url = f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAPKEY}/VIIRS_SNPP_NRT/SDN/10/{current_date}"
    fetch_and_save_data(snpp_url, "snpp")

def fetch_modis_data():
    modis_url = f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAPKEY}/MODIS_NRT/SDN/10/{current_date}"
    fetch_and_save_data(modis_url, "modis")

def fetch_modis_sp_data():
    modis_sp_url = f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAPKEY}/MODIS_SP/SDN/10/{current_date}"
    fetch_and_save_data(modis_sp_url, "modis_sp")
    
while True:
    #schedule.run_pending()
    #time.sleep(1)
    fetch_noaa20_data()
    fetch_snpp_data()
    fetch_modis_data()
    fetch_noaa21_data()
    fetch_snpp_sp_data()
    fetch_modis_sp_data()
    current_date = increment_date(current_date, days_to_increment)
    time.sleep(600)
