import json
from pymongo import MongoClient
import geopandas as gpd
from pymongo.database import Collection, Database
from shapely.geometry import shape
import matplotlib.pyplot as plt
import pandas as pd
from dataframeCreation import create_main_dataframe, create_powiaty, create_wojewodztwa


def create_json_list_from_gdf(gdf: gpd.GeoDataFrame) -> list[dict]:
    gdf_json = gdf.to_json(to_wgs84=True)
    gdf_json_list = json.loads(gdf_json)["features"]

    final_list = []
    for element in gdf_json_list:
        final_list.append(element["properties"])
        final_list[-1]["geometry"] = element["geometry"]
    return final_list


def insert_stacje_data(stacje_collection: Collection) -> None:
    stacje = create_main_dataframe()
    stacje["date"] = stacje["date"].astype(str)

    lista_do_zapisu = create_json_list_from_gdf(stacje)
    print(lista_do_zapisu)
    stacje_collection.insert_many(lista_do_zapisu)
    
def insert_sorted_data(client: MongoClient = None, gdf: gpd.GeoDataFrame = None) -> None:
    # Inserts data to several databases 
    # db_woj = client['wojewodztwa']
    # db_powiat = client['powiaty']
    # db_stacja = client['stacje']
    
    
    # db_dzien = client['dni']
    # db_noc = client['noce']
    # db_doba = client['doby']
    
    if gdf == None:
        gdf = create_main_dataframe()
        gdf["date"] = gdf["date"].astype(str)
        
    group_woj = gdf.groupby("name_woj")
    group_powiat = gdf.groupby('name_pow')
    group_station = gdf.groupby('ifcid')
    
    # For date-time > maybe divide it to 2 attributes? Date and hour independently
    
    
    print(grouped.groups.keys()) # Get all keys. For it to be iterable, convert to list()
    
    
    


def insert_gdf(collection: Collection, gdf: gpd.GeoDataFrame) -> None:
    lista_do_zapisu = create_json_list_from_gdf(gdf)
    collection.insert_many(lista_do_zapisu)


def read_collection_data(stacje_collection: Collection) -> gpd.GeoDataFrame:
    lista_stacji = list(stacje_collection.find({}))
    gdf = gpd.GeoDataFrame.from_dict(lista_stacji, geometry=[shape(stacja["geometry"]) for stacja in lista_stacji],
                                     crs=4326).to_crs(2180)
    return gdf

def connect_to_mongodb(credentials: str) -> MongoClient:
    return MongoClient(credentials)


if __name__ == '__main__':
    client: MongoClient = MongoClient("mongodb+srv://haslo:haslo@cluster0.ejzrvjx.mongodb.net/")
    db: Database = client.daneIMGW
    opady: Collection = db.opady
    obserwacje = dict()
    # print(opady.find_one())
    # for wojewodztwo in opady.distinct("properties.name_woj"):
    #     print(f"Liczymy wojewodztwo: {wojewodztwo}")
    #     obserwacje[wojewodztwo] = [obserwacja["properties.value"] for obserwacja in opady.find({"properties.date": "2023-09-13 06:00:00", "properties.name_woj": wojewodztwo})]
    #     print("Obserwacje: ", obserwacje[wojewodztwo])
    # print()
    df = pd.DataFrame.from_dict(obserwacje)
    print(df)
    print(df.shape)
    
    insert_sorted_data()
    
    # insert_stacje_data(client["test"]["coll"])
    # df = pd.DataFrame(obserwacje)
    # print(df)
    # gdf1 = read_collection_data(wojewodztwa)
    # gdf2 = read_collection_data(powiaty)
    # gdf3 = read_collection_data(stacje)

    # gdf1.plot()
    # gdf2.plot()
    # gdf3.plot()
    # plt.show()

    client.close()
