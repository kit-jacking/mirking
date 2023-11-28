import json
from pymongo import MongoClient
import geopandas as gpd
from pymongo.database import Collection, Database
from shapely.geometry import shape
import matplotlib.pyplot as plt

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

    stacje_collection.insert_many(lista_do_zapisu)


def insert_gdf(collection: Collection, gdf: gpd.GeoDataFrame) -> None:
    lista_do_zapisu = create_json_list_from_gdf(gdf)
    collection.insert_many(lista_do_zapisu)

def read_collection_data(stacje_collection: Collection) -> gpd.GeoDataFrame:
    lista_stacji = list(stacje_collection.find({}))
    gdf = gpd.GeoDataFrame.from_dict(lista_stacji, geometry=[shape(stacja["geometry"]) for stacja in lista_stacji],
                                     crs=4326).to_crs(2180)
    return gdf


if __name__ == '__main__':
    client: MongoClient = MongoClient("mongodb://localhost:8081")
    db: Database = client.db
    wojewodztwa = db.wojewodztwa
    powiaty = db.powiaty
    stacje = db.stacje

    gdf1 = read_collection_data(wojewodztwa)
    gdf2 = read_collection_data(powiaty)
    gdf3 = read_collection_data(stacje)

    gdf1.plot()
    gdf2.plot()
    gdf3.plot()
    plt.show()

    client.close()
