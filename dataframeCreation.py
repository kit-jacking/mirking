from datetime import datetime

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from shapely import to_geojson


def create_main_dataframe() -> gpd.GeoDataFrame:
    powiaty = create_powiaty()
    woj = create_wojewodztwa()
    effacility = create_effacility()

    opady_dzienne = pd.read_csv(r"C:\Users\qattr\Desktop\STUD\SEM 5\PAG\Projekt-2\Dane\Dane-IMGW\2023-10\B00604S_2023_10.csv",
                                header=None,
                                delimiter=";").rename(columns={0: "ifcid", 1: "type", 2: "date", 3: "value"})
    
    opady_dzienne['value'] = opady_dzienne['value'].str.replace(',', '.').astype(float)
    opady_dzienne["date"] = pd.to_datetime(opady_dzienne["date"])

    stacje_zlaczone = opady_dzienne.merge(effacility[["ifcid", "geometry", "name1"]], how="left", on="ifcid")
    stacje_zlaczone = gpd.GeoDataFrame(stacje_zlaczone, geometry=stacje_zlaczone["geometry"]).to_crs(epsg=2180)
    
    stacje_zlaczone = gpd.tools.sjoin(stacje_zlaczone, woj[["geometry", "name"]], how="left", rsuffix="woj")
    stacje_zlaczone = gpd.tools.sjoin(stacje_zlaczone, powiaty[["geometry", "name"]], how="left", rsuffix="pow").rename(
        columns={"name_left": "name_woj", 'name1':"name_eff"})
    stacje_zlaczone = stacje_zlaczone.drop(columns=[4])
    stacje_zlaczone = stacje_zlaczone.dropna()
    return stacje_zlaczone, effacility, powiaty, woj

def create_effacility() -> gpd.GeoDataFrame:
    eff = gpd.read_file(r"C:\Users\qattr\Desktop\STUD\SEM 5\PAG\Projekt-2\Dane\effacility.geojson")
    eff.crs = 2180
    return eff

def create_wojewodztwa() -> gpd.GeoDataFrame:
    woj = gpd.read_file(r"C:\Users\qattr\Desktop\STUD\SEM 5\PAG\Projekt-2\Dane\woj.shp").to_crs(epsg=2180)
    woj = woj[['name', 'geometry']]
    # woj['geom_woj'] = woj.geometry
    return woj


def create_powiaty() -> gpd.GeoDataFrame:
    powiaty = gpd.read_file(r"C:\Users\qattr\Desktop\STUD\SEM 5\PAG\Projekt-2\Dane\powiaty.shp").to_crs(epsg=2180)
    powiaty = powiaty[['name', 'geometry']]
    # powiaty['geom_pow'] = powiaty.geometry
    return powiaty


def create_dataframes(main_geodataframe: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    # Srednia i mediana wartości pomiaru w podziale na daty w poszczegolnych wojewodztwach, powiatach i jednostkach:

    opady_woj = main_geodataframe.groupby(["name_woj", "date"])["value"].aggregate(["mean", "median"])
    opady_pow = main_geodataframe.groupby(["name_pow", "date"])["value"].aggregate(["mean", "median"])
    opady_eff = main_geodataframe.groupby(["ifcid", "date"])["value"].aggregate(["mean", "median"])

    # # Zmiany wartosci sredniej i mediany w zadanych interwalach czasu w wojewodztwach i powiatach.
    # date_format = "%Y-%m-%d %H:%M"
    # start = datetime.strptime("2023-09-01 06:00", date_format)
    # end = datetime.strptime("2023-09-04 06:00", date_format)

    # opady_woj_daty = \
    #     stacje_zlaczone.loc[(stacje_zlaczone["date"] >= start) & (stacje_zlaczone["date"] < end)].groupby(
    #         ["name_woj", "date"])[
    #         "value"].aggregate(["mean", "median"])
    # opady_pow_daty = \
    #     stacje_zlaczone.loc[(stacje_zlaczone["date"] >= start) & (stacje_zlaczone["date"] < end)].groupby(
    #         ["name_pow", "date"])[
    #         "value"].aggregate(["mean", "median"])

    return opady_woj, opady_pow, opady_eff
                                          #, opady_woj_daty, opady_pow_daty


if __name__ == '__main__':
    pass
    # import json
    # xs = pd.IndexSlice
    
    # main_gdf, eff_gdf, pow_gdf, woj_gdf = create_main_dataframe()
    # main_gdf["date"] = main_gdf["date"].astype(str)
    # list_eff = []
    # opady_woj, opady_pow, opady_eff = create_dataframes(main_gdf)
    # # print(type(eff_gdf["ifcid"][0]))
    # # woj_gdf = woj_gdf[['name', 'geometry']]
    # # print(woj_gdf)
    # print(opady_eff.index.values.tolist())
    # for i in eff_gdf.index:
    #     eff = eff_gdf["ifcid"][i]
    #     name = eff_gdf['name1'][i]
    #     print(eff, name)
    #     try:
    #         geom = json.loads(to_geojson(eff_gdf["geometry"][i]))
    #         mean = opady_eff.loc[xs[eff,:], :].reset_index([0])["mean"].to_dict()
    #         median = opady_eff.loc[xs[eff,:], :].reset_index([0])["median"].to_dict()
    #         json_dict = {"ifcid": eff, "name": name, "mean": mean, "median": median, "geometry": geom}
    #         list_eff.append(json_dict)
    #     except:
    #         print("??? Nie ma stacji: ", eff)
    #         continue
    
    # create_main_dataframe()
    # a,b = create_dataframes()
    
    # print(a)
    # print()
    # print(b)
    
    # Wyciaganie danych
    
    # print(a.loc[xs[:,'2023-10-01 06:00:00'], :])