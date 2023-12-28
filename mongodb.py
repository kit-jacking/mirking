import json
from pymongo import MongoClient
import geopandas as gpd
from pymongo.database import Collection, Database
from shapely.geometry import shape
from shapely import to_geojson
import matplotlib.pyplot as plt
import pandas as pd
from dataframeCreation import create_dataframes, create_main_dataframe, create_powiaty, create_wojewodztwa


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
    
# def create_woj_data(woj_gdf: gpd.GeoDataFrame ,stat_gdf: gpd.GeoDataFrame):
#     woj_gdf = woj_gdf[['name', 'geometry']]
    
def insert_sorted_data(client: MongoClient = None, main_gdf: gpd.GeoDataFrame = None, eff_gdf: gpd.GeoDataFrame = None, pow_gdf: gpd.GeoDataFrame = None, woj_gdf: gpd.GeoDataFrame = None ) -> None:
    # Inserts data to several databases 
    db_woj = client.grouped_data.wojewodztwa
    db_pow = client.grouped_data.powiaty
    db_eff = client.grouped_data.stacje
    
    # db_dzien = client['dni']
    # db_noc = client['noce']
    # db_doba = client['doby']
    
    # Create list of jsons to add to each db
    list_woj = []
    list_pow = []
    list_eff = []
    
    if main_gdf == None:
        main_gdf, eff_gdf, pow_gdf, woj_gdf = create_main_dataframe()
        main_gdf["date"] = main_gdf["date"].astype(str)
        
    opady_woj, opady_pow, opady_eff = create_dataframes(main_gdf)
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
    
    # Insert stacje to db
    print("Inserting effacilities...")
    for i in eff_gdf.index:
        eff = eff_gdf["ifcid"][i]
        name = eff_gdf['name1'][i]
        try:
            geom = json.loads(to_geojson(eff_gdf["geometry"][i]))
            mean = opady_eff.loc[xs[eff,:], :].reset_index([0])["mean"].to_dict()
            median = opady_eff.loc[xs[eff,:], :].reset_index([0])["median"].to_dict()
            eff = str(eff)
            json_dict = {"ifcid": eff, "name": name, "mean": mean, "median": median, "geometry": geom}
            list_eff.append(json_dict)
        except:
            print("Nie ma stacji: ", eff)
            continue
    
    db_eff.insert_many(list_eff)
    
    

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
    # df = pd.DataFrame.from_dict(obserwacje)
    # print(df)
    # print(df.shape)
    
    insert_sorted_data(client)
    
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
