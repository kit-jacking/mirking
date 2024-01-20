from importlib import simple
import json
from pymongo import MongoClient
import geopandas as gpd
from pymongo.database import Collection, Database
from shapely.geometry import shape
from shapely import to_geojson
import matplotlib.pyplot as plt
import pandas as pd
from dataframeCreation import create_dataframes, create_geodataframes, create_main_dataframes
from imgwDownloader import download_data

MONTH = '10'
YEAR = '2023'
DEF_HOUR = '06'
global path

def create_json_list_from_gdf(gdf: gpd.GeoDataFrame) -> list[dict]:
    gdf_json = gdf.to_json(to_wgs84=True)
    gdf_json_list = json.loads(gdf_json)["features"]

    final_list = []
    for element in gdf_json_list:
        final_list.append(element)
        final_list[-1]["geometry"] = element["geometry"]
    return final_list
    
# ***
# Dodanie geometrii woj i pow
def insert_jst_geometries(client: MongoClient, pow_geom_gdf: gpd.GeoDataFrame, woj_geom_gdf: gpd.GeoDataFrame):
    db_woj_geom = client.geometries.wojewodztwa
    db_pow_geom = client.geometries.powiaty
    
    list_woj, list_pow = [], []
    size = woj_geom_gdf.shape[0]
    for i in range(size - 1):
        json_dict = {"name": woj_geom_gdf['name'][i], 'geometry':json.loads(to_geojson(woj_geom_gdf["geometry"][i]))}
        list_woj.append(json_dict)
    size = pow_geom_gdf.shape[0]    
    for i in range(size - 1):
        json_dict = {"name": pow_geom_gdf['name'][i], 'geometry':json.loads(to_geojson(pow_geom_gdf["geometry"][i]))}
        list_pow.append(json_dict)
    
    print("Inserting woj...")
    db_woj_geom.insert_many(list_woj)
    print("Indexing...")
    db_woj_geom.create_index(["geometry", "2dsphere"])
    print("Inserting pow...")
    db_pow_geom.insert_many(list_pow)
    print("Indexing...")
    db_pow_geom.create_index(["geometry", "2dsphere"])
    print("Finished")
    

# ***
# Dodanie posortowanych danych do Mongo 
def insert_sorted_data(client: MongoClient) -> None:
    # Inserts data to several databases 
    db_opady = client.daneIMGW.TTTopady
    db_woj = client.grouped_data.wojewodztwa
    db_pow = client.grouped_data.powiaty
    
    db_dzien = client.grouped_data.dni
    db_noc = client.grouped_data.noce
    db_doba = client.grouped_data.doby
    
    # Create list of jsons to add to each db
    list_woj = []
    list_pow = []
    list_dni, list_noce, list_doby = [], [], []
    
    
    raw_day, raw_min, main_gdf, eff_gdf, pow_gdf, woj_gdf = create_main_dataframes()
    opady_daytime, opady_nighttime, opady_day = create_dataframes(raw_min)
    opady_woj, opady_pow = create_geodataframes(main_gdf)
    

    insert_jst_geometries(client, pow_gdf, woj_gdf)
    
    main_gdf["date"] = main_gdf["date"].astype(str)
    
    # Add default opady data
    insert_gdf(db_opady, main_gdf)

    xs = pd.IndexSlice

    # Insert wojewodztwa to db
    print("Inserting voivodeships...")
    for i in woj_gdf.index:
        woj = woj_gdf["name"][i]
        geom = json.loads(to_geojson(woj_gdf["geometry"][i]))
        mean = opady_woj.loc[xs[woj,:], :].reset_index([0])["mean"].to_dict()
        median = opady_woj.loc[xs[woj,:], :].reset_index([0])["median"].to_dict()
        json_dict = {"name": woj, "mean": mean, "median": median, "geometry": geom}
        list_woj.append(json_dict)
    
    db_woj.insert_many(list_woj)
    
    # Insert powiaty to db
    print("Inserting powiaty...")
    for i in pow_gdf.index:
        pow = pow_gdf["name"][i]
        geom = json.loads(to_geojson(pow_gdf["geometry"][i]))
        try:
            mean = opady_pow.loc[xs[pow,:], :].reset_index([0])["mean"].to_dict()
            median = opady_pow.loc[xs[pow,:], :].reset_index([0])["median"].to_dict()
            json_dict = {"name": pow, "mean": mean, "median": median, "geometry": geom}
            list_pow.append(json_dict)
        except:
            print("Powiat bez stacji")
            continue
    
    db_pow.insert_many(list_pow)
    
    # Insert dni to db
    print("Inserting dni...")
    print(opady_daytime)
    for i in opady_daytime.index:
        mean = opady_daytime.loc[i]['mean']
        median = opady_daytime.loc[i]['median']
        json_dict = {'date': str(i), 'mean': mean, 'median': median}
        list_dni.append(json_dict)
        
    db_dzien.insert_many(list_dni)
    
    # Insert noce to db
    print("Inserting noce...")
    for i in opady_nighttime.index:
        mean = opady_nighttime.loc[i]['mean']
        median = opady_nighttime.loc[i]['median']
        json_dict = {'date': str(i), 'mean': mean, 'median': median}
        list_noce.append(json_dict)
        
    db_noc.insert_many(list_noce)
    
    # Insert doby to db
    print("Inserting doby...")
    for i in opady_day.index:
        mean = opady_day.loc[i]['mean']
        median = opady_day.loc[i]['median']
        json_dict = {'date': str(i), 'mean': mean, 'median': median}
        list_doby.append(json_dict)
        
    db_doba.insert_many(list_doby)
    

def insert_gdf(collection: Collection, gdf: gpd.GeoDataFrame) -> None:
    lista_do_zapisu = create_json_list_from_gdf(gdf)
    collection.insert_many(lista_do_zapisu)


def read_collection_data(stacje_collection: Collection) -> gpd.GeoDataFrame:
    lista_stacji = list(stacje_collection.find({}))
    gdf = gpd.GeoDataFrame.from_dict(lista_stacji, geometry=[shape(stacja["geometry"]) for stacja in lista_stacji],
                                     crs=4326).to_crs(2180)
    return gdf

# *** FUNKCJA POZWALA NA UZYSKANIE ŚREDNIEJ OPADÓW W SKALI MIESIĄCA DLA DANEJ JEDNOSTKI JST - OBLICZENIA DZIEJĄ SIĘ W BAZIE
# nie wiem czemu tylko median nie działa - niby jest $median
def calculate_mean_within_jst(client: MongoClient, jst_name: str, is_woj: bool):
    if is_woj: 
        geom = client.geometries.wojewodztwa.find_one({"name": jst_name},{'geometry':1, '_id':0})
    else:
        geom = client.geometries.powiaty.find_one({"name": jst_name},{'geometry':1, '_id':0})
        
    print(jst_name)

    result = client.daneIMGW.opady.aggregate([
        {
            "$match": {
                "geometry": {
                    "$geoWithin": {
                        "$geometry": geom['geometry']
                    }
                }
            }
        },
        {
            "$group": {
                "_id": None,
                # "MEAN": {"$avg": "$value"}
                "MEAN": {"$avg": "$properties.value"}
            }
        }
    ])
    # cursor = opady.find_one({"geometry": {"$geoWithin": {"$geometry": geom['geometry']}}})['properties']['value']
    for r in result:
        print(r)
    
    
# FUNKCJA ZWRACA LISTĘ WARTOŚCI OPADÓW DLA PRZEDZIAŁU DAT DLA JST
def read_station_data_within_jst(client: MongoClient, jst_name: str, is_woj: bool, day_start: int = 0, day_end: int = 0):
    if is_woj: 
        geom = client.geometries.wojewodztwa.find_one({"name": jst_name},{'geometry':1, '_id':0})
    else:
        geom = client.geometries.powiaty.find_one({"name": jst_name},{'geometry':1, '_id':0})
        
    if day_start == 0 or day_end == 0: # Zwraca dla całego miesiąca
        cursor = client.daneIMGW.opady.find({"geometry": {"$geoWithin": {"$geometry": geom['geometry']}}})['properties']['value']
    else:
        start_date = f"{YEAR}-{MONTH}-{day_start:02} {DEF_HOUR}:00"
        end_date = f"{YEAR}-{MONTH}-{day_end:02} {DEF_HOUR}:00"
        dates = [start_date]
        for i in range(day_end - day_start - 1):
            dates.append(f"{YEAR}-{MONTH}-{(i+day_start+1):02} {DEF_HOUR}:00")
        dates.append(end_date)
        cursor = client.daneIMGW.opady.find({"$and": [{"geometry": {"$geoWithin": {"$geometry": geom['geometry']}}},{"properties.date": {"$in": dates}}]})
    dict_result = {}
    for r in cursor:
        dict_result[r['properties']['date']] = r['properties']['value']
    return dict_result
    
def connect_to_mongodb(credentials: str) -> MongoClient:
    return MongoClient(credentials)


def jst_input():
    jst_name = input("Podaj nazwę JST: ")
    is_woj = input('Czy jest to wojewodztwo? [Y/n]')
    if is_woj == '' or is_woj == 'Y' or is_woj == 'Y':
        is_woj = True
    elif is_woj == 'n' or is_woj == 'N':
        is_woj = False
    else:
        print('Nieznana komenda')
        simple_gui()
    return jst_name, is_woj


def download_sorted_data_country(client: MongoClient, date: int, type: str):

    dt = f'{YEAR}-{MONTH}-{date:02} 00:00:00'
    if type == '1':
        res = client.grouped_data.dni.find_one({"date":dt})
    elif type == '2':
        res = client.grouped_data.noce.find_one({"date":dt})
    elif type == '3':
        res = client.grouped_data.doby.find_one({"date":dt})
    print(res)
    
def download_sorted_data_jst(client: MongoClient, date: int, type: str, jst_name: str):
    whole = False
    if date == 0:
        whole = True
    else:
        dt = f'{YEAR}-{MONTH}-{date:02} 06:00:00'
    if type == '4':
        res = client.grouped_data.powiaty.find_one({'name': jst_name})
        if whole:
            print(res)
        else:
            print(f"Powiat: {jst_name} Data: {YEAR}-{MONTH}-{date:02}\nŚrednia: {res['mean'][dt]}\nMediana: {res['median'][dt]}")
    elif type == '5':
        res = client.grouped_data.wojewodztwa.find_one({'name': jst_name})
        if whole:
            print(res)
        else:
            print(f"Województwo: {jst_name} Data: {YEAR}-{MONTH}-{date:02}\nŚrednia: {res['mean'][dt]}\nMediana: {res['median'][dt]}")
            
        
        
def simple_gui(cred):
    try:
        client = MongoClient(cred)
    except:
        print('Błąd połączenia z bazą')
        return
    
    print("MENU\n(1) Dodaj dane do bazy (pobierz, przetwórz, dodaj)\n(2) Oblicz średnią miesięczną opadów w JST\n(3) Zwróć wartości opadów dla JST w przedziale dat\n(4) Zwróć dane wstępnie posortowane\n(x) Zakończ")
    chosen_menu = input()
    if chosen_menu == "1":
        path = input("Podaj ścieżkę do pobrania danych z IMGW: ")
        download_data(2023, 10, rf'{path}')
        # C:\Users\qattr\Desktop\STUD\SEM 5\PAG\Projekt-2\Dane\Dane-IMGW
        insert_sorted_data(client)
    elif chosen_menu == '2':
        jst_name, is_woj = jst_input()
        calculate_mean_within_jst(client, jst_name, is_woj)
    elif chosen_menu == '3':
        jst_name, is_woj = jst_input()
        day_start = input("Podaj dzień początkowy [domyślnie: 1]")
        day_end = input("Podaj dzień końcowy [domyślnie: 1]")
        day_start = 1 if day_start == '' else int(day_start)
        day_end = 1 if day_end == '' else int(day_end)
        read_station_data_within_jst(client, jst_name, is_woj, day_start, day_end)
    elif chosen_menu == '4':
        option = input("Wybierz dane:\n(1) Średnia i mediana opadów w ciągu dnia na terenie Polski\n(2) Średnia i mediana opadów w ciągu nocy na terenie Polski\n(3) Średnia i mediana opadów w ciągu doby na terenie Polski\n(4) Średnia i mediana opadów w ciągu doby na terenie powiatu\n(5) Średnia i mediana opadów w ciągu doby na terenie województwa\n> ")
        if option == '1':
            day = input("Podaj dzień\n> ")
            download_sorted_data_country(client, day, '1')
        elif option == '2':
            day = input("Podaj dzień\n> ")
            download_sorted_data_country(client, day, '2')
        elif option == '3':
            day = input("Podaj dzień\n> ")
            download_sorted_data_country(client, day, '3')
        elif option == '4':
            day = input("Podaj dzień. Wpisz '0' aby pobrać cały dokument.\n> ")
            jst = input("Podaj nazwę powiatu\n> ")
            download_sorted_data_jst(client, day, '4', jst)
        elif option == '5':   
            day = input("Podaj dzień. Wpisz '0' aby pobrać cały dokument.\n> ")
            jst = input("Podaj nazwę województwa\n> ")
            download_sorted_data_jst(client, day, '5', jst)
    
    
    
    
if __name__ == '__main__':
    client: MongoClient = MongoClient("mongodb+srv://haslo:haslo@cluster0.ejzrvjx.mongodb.net/")
    db: Database = client.daneIMGW
    opady: Collection = db.TTopady
    
    # client_cred = input("Podaj link do połączenia do bazy: ")
    # insert_sorted_data(client)
    # simple_gui(client_cred)
    download_sorted_data_jst(client, 0, '4', "złotoryjski")
    # print(read_station_data_within_jst(client, 'Rybnik', False,1,5))
    
    # DODANIE OPADOW!!!!

    # client.close()
