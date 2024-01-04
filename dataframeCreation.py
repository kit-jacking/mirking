from datetime import datetime

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd


def create_main_dataframe() -> gpd.GeoDataFrame:
    powiaty = gpd.read_file("dane/powiaty.shp").to_crs(epsg=2180)
    woj = gpd.read_file("dane/woj.shp").to_crs(epsg=2180)

    effacility = gpd.read_file("dane/effacility.geojson")
    effacility.crs = 2180
    opady_dzienne = pd.read_csv("dane/Meteo_2023-09/B00604S_2023_09.csv",
                                header=None,
                                delimiter=";").rename(columns={0: "ifcid", 1: "type", 2: "date", 3: "value"})
    opady_dzienne['value'] = opady_dzienne['value'].str.replace(',', '.').astype(float)
    opady_dzienne["date"] = pd.to_datetime(opady_dzienne["date"])

    stacje_zlaczone = opady_dzienne.merge(effacility[["ifcid", "geometry"]], how="left", on="ifcid")
    stacje_zlaczone = gpd.GeoDataFrame(stacje_zlaczone, geometry=stacje_zlaczone["geometry"]).to_crs(epsg=2180)

    stacje_zlaczone = gpd.tools.sjoin(stacje_zlaczone, woj[["geometry", "name"]], how="left", rsuffix="woj")
    stacje_zlaczone = gpd.tools.sjoin(stacje_zlaczone, powiaty[["geometry", "name"]], how="left", rsuffix="pow").rename(
        columns={"name_left": "name_woj"})
    stacje_zlaczone = stacje_zlaczone.drop(columns=[4])
    stacje_zlaczone = stacje_zlaczone.dropna()
    return stacje_zlaczone


def create_wojewodztwa() -> gpd.GeoDataFrame:
    woj = gpd.read_file("dane/woj.shp").to_crs(epsg=2180)
    return woj


def create_powiaty() -> gpd.GeoDataFrame:
    powiaty = gpd.read_file("dane/powiaty.shp").to_crs(epsg=2180)
    return powiaty


def create_dataframes() -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    stacje_zlaczone = create_main_dataframe()

    # Srednia i mediana wartości pomiaru w podziale na daty w poszczegolnych wojewodztwach i powiatach:

    opady_woj = stacje_zlaczone.groupby(["name_woj", "date"])["value"].aggregate(["mean", "median"])
    opady_pow = stacje_zlaczone.groupby(["name_pow", "date"])["value"].aggregate(["mean", "median"])

    # print(opady_pow)
    # print(opady_woj)

    # Zmiany wartosci sredniej i mediany w zadanych interwalach czasu w wojewodztwach i powiatach.

    date_format = "%Y-%m-%d %H:%M"
    start = datetime.strptime("2023-09-01 06:00", date_format)
    end = datetime.strptime("2023-09-04 06:00", date_format)

    opady_woj_daty = \
        stacje_zlaczone.loc[(stacje_zlaczone["date"] >= start) & (stacje_zlaczone["date"] < end)].groupby(
            ["name_woj", "date"])[
            "value"].aggregate(["mean", "median"])
    opady_pow_daty = \
        stacje_zlaczone.loc[(stacje_zlaczone["date"] >= start) & (stacje_zlaczone["date"] < end)].groupby(
            ["name_pow", "date"])[
            "value"].aggregate(["mean", "median"])

    # print(opady_woj_daty)
    # print(opady_pow_daty)

    return opady_woj, opady_pow, opady_woj_daty, opady_pow_daty


if __name__ == '__main__':
    #stacje_zlaczone = create_main_dataframe()
    powiaty = create_powiaty()
    print(powiaty.columns)
    print(powiaty['national_c'])

    '''wojewodztwa = create_wojewodztwa()
    print(wojewodztwa.to_string())
    print("--------")
    print(stacje_zlaczone.to_string())
    print("--------")'''
    #stacje_zlaczone.plot()
    #plt.show()