import json
from pymongo import MongoClient
import geopandas as gpd
from pymongo.database import Collection, Database
from shapely.geometry import shape
from shapely import to_geojson
import matplotlib.pyplot as plt
import pandas as pd
from dataframeCreation import create_dataframes, create_geodataframes, create_main_dataframes, create_powiaty, create_wojewodztwa

MONTH = '10'
YEAR = '2023'
DEF_HOUR = '06'


def create_json_list_from_gdf(gdf: gpd.GeoDataFrame) -> list[dict]:
    gdf_json = gdf.to_json(to_wgs84=True)
    gdf_json_list = json.loads(gdf_json)["features"]

    final_list = []
    for element in gdf_json_list:
        final_list.append(element["properties"])
        final_list[-1]["geometry"] = element["geometry"]
    return final_list

# ***
# Dodanie surowych?
def insert_stacje_data(stacje_collection: Collection) -> None:
    stacje = create_main_dataframes()
    stacje["date"] = stacje["date"].astype(str)

    lista_do_zapisu = create_json_list_from_gdf(stacje)
    print(lista_do_zapisu)
    stacje_collection.insert_many(lista_do_zapisu)
    
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
def insert_sorted_data(client: MongoClient, main_gdf: gpd.GeoDataFrame = None, main_df: pd.DataFrame = None, pow_gdf: gpd.GeoDataFrame = None, woj_gdf: gpd.GeoDataFrame = None) -> None:
    # Inserts data to several databases 
    db_woj = client.grouped_data.wojewodztwa
    db_pow = client.grouped_data.powiaty
    
    db_dzien = client.grouped_data.dni
    db_noc = client.grouped_data.noce
    db_doba = client.grouped_data.doby
    
    # Create list of jsons to add to each db
    list_woj = []
    list_pow = []
    
    list_dni, list_noce, list_doby = [], [], []
    
    if main_gdf == None or main_df == None:
        raw_day, raw_min, main_gdf, eff_gdf, pow_gdf, woj_gdf = create_main_dataframes()
        main_gdf["date"] = main_gdf["date"].astype(str)
    
        opady_daytime, opady_nighttime, opady_day = create_dataframes(raw_min)
        # opady_daytime["date"] = opady_daytime["date"].astype(str)
        # opady_nighttime["date"] = opady_nighttime["date"].astype(str)
        # opady_day["date"] = opady_day["date"].astype(str)
        
    opady_woj, opady_pow = create_geodataframes(main_gdf)
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
    for i in opady_daytime.index:
        mean = opady_daytime.loc[i]['mean']
        median = opady_daytime.loc[i]['median']
        json_dict = {'date': i, 'mean': mean, 'median': median}
        list_dni.append(json_dict)
        
    db_dzien.insert_many(list_dni)
    
    # Insert noce to db
    print("Inserting noce...")
    for i in opady_nighttime.index:
        mean = opady_nighttime.loc[i]['mean']
        median = opady_nighttime.loc[i]['median']
        json_dict = {'date': i, 'mean': mean, 'median': median}
        list_noce.append(json_dict)
        
    db_noc.insert_many(list_noce)
    
    # Insert doby to db
    print("Inserting doby...")
    for i in opady_day.index:
        mean = opady_day.loc[i]['mean']
        median = opady_day.loc[i]['median']
        json_dict = {'date': i, 'mean': mean, 'median': median}
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
        cursor = opady.find({"geometry": {"$geoWithin": {"$geometry": geom['geometry']}}})['properties']['value']
    else:
        start_date = f"{YEAR}-{MONTH}-{day_start:02} {DEF_HOUR}:00"
        end_date = f"{YEAR}-{MONTH}-{day_end:02} {DEF_HOUR}:00"
        dates = [start_date]
        for i in range(day_end - day_start - 1):
            dates.append(f"{YEAR}-{MONTH}-{(i+day_start+1):02} {DEF_HOUR}:00")
        dates.append(end_date)
        cursor = client.daneIMGW.opady.find({"$and": [{"geometry": {"$geoWithin": {"$geometry": geom['geometry']}}},{"properties.date": {"$in": dates}}]})
        
    for r in cursor:
        print(r)
    
def connect_to_mongodb(credentials: str) -> MongoClient:
    return MongoClient(credentials)


if __name__ == '__main__':
    client: MongoClient = MongoClient("mongodb+srv://haslo:haslo@cluster0.ejzrvjx.mongodb.net/")
    db: Database = client.daneIMGW
    opady: Collection = db.opady

    read_station_data_within_jst(client, 'Rybnik', False,1,5)

    client.close()
