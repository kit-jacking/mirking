from datetime import datetime

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import scipy
from shapely import to_geojson


def create_main_dataframes() -> gpd.GeoDataFrame:
    powiaty = create_powiaty()
    woj = create_wojewodztwa()
    effacility = create_effacility()

    raw_data_days = pd.read_csv(r"C:\Users\qattr\Desktop\STUD\SEM 5\PAG\Projekt-2\Dane\Dane-IMGW\2023-10\B00604S_2023_10.csv",
                                header=None,
                                delimiter=";").rename(columns={0: "ifcid", 1: "type", 2: "date", 3: "value"})
    raw_data_minutes = pd.read_csv(r"C:\Users\qattr\Desktop\STUD\SEM 5\PAG\Projekt-2\Dane\Dane-IMGW\2023-10\B00608S_2023_10.csv",
                                header=None,
                                delimiter=";").rename(columns={0: "ifcid", 1: "type", 2: "date", 3: "value"})
    
    raw_data_days['value'] = raw_data_days['value'].str.replace(',', '.').astype(float)
    raw_data_days["date"] = pd.to_datetime(raw_data_days["date"])
    
    # Przygotowanie do analizy stat
    raw_data_minutes['value'] = raw_data_minutes['value'].str.replace(',', '.').astype(float)
    raw_data_minutes["date"] = pd.to_datetime(raw_data_minutes["date"])
    raw_data_minutes = raw_data_minutes.drop(columns=[4])
    
    def is_day_filter(hr):
        if hr >= 6 and hr < 22:
            return True
        else:
            return False
        
    raw_data_minutes['is_day'] = raw_data_minutes.date.dt.hour.map(is_day_filter)

    # Przygotowanie do analizy geostat
    stacje_zlaczone = raw_data_days.merge(effacility[["ifcid", "geometry", "name1"]], how="left", on="ifcid")
    stacje_zlaczone = gpd.GeoDataFrame(stacje_zlaczone, geometry=stacje_zlaczone["geometry"]).to_crs(epsg=2180)
    
    stacje_zlaczone = gpd.tools.sjoin(stacje_zlaczone, woj[["geometry", "name"]], how="left", rsuffix="woj")
    stacje_zlaczone = gpd.tools.sjoin(stacje_zlaczone, powiaty[["geometry", "name"]], how="left", rsuffix="pow").rename(
        columns={"name_left": "name_woj", 'name1':"name_eff"})
    stacje_zlaczone = stacje_zlaczone.drop(columns=[4])
    stacje_zlaczone = stacje_zlaczone.dropna()
    
    return raw_data_days, raw_data_minutes, stacje_zlaczone, effacility, powiaty, woj

def create_effacility() -> gpd.GeoDataFrame:
    eff = gpd.read_file(r"C:\Users\qattr\Desktop\STUD\SEM 5\PAG\Projekt-2\Dane\effacility.geojson")
    eff.crs = 2180
    return eff

def create_wojewodztwa() -> gpd.GeoDataFrame:
    woj = gpd.read_file(r"C:\Users\qattr\Desktop\STUD\SEM 5\PAG\Projekt-2\Dane\woj.shp").to_crs(4326)
    woj = woj[['name', 'geometry']]
    return woj


def create_powiaty() -> gpd.GeoDataFrame:
    powiaty = gpd.read_file(r"C:\Users\qattr\Desktop\STUD\SEM 5\PAG\Projekt-2\Dane\powiaty.shp").to_crs(4326)
    powiaty = powiaty[['name', 'geometry']]
    return powiaty

# *** ANALIZA STATYSTYCZNA

def create_dataframes(main_dataframe: pd.DataFrame):
    opady_daytime = main_dataframe.loc[main_dataframe['is_day'] == True].groupby(pd.Grouper(key='date', freq='D'))['value'].aggregate(["mean", "median"])
    opady_nighttime = main_dataframe.loc[main_dataframe['is_day'] == False].groupby(pd.Grouper(key='date', freq='D'))['value'].aggregate(["mean", "median"])
    opady_day = main_dataframe.groupby(pd.Grouper(key='date', freq='D'))['value'].aggregate(["mean", "median"])
    # add maybe also group by effacility?
    
    # Średnia odcięta kod, który nie działa z aggregate: scipy.stats.trim_mean(main_dataframe.loc[main_dataframe['is_day'] == True].value, 0.1)
    return opady_daytime, opady_nighttime, opady_day 


# *** ANALIZA GEOSTATYSTYCZNA

def create_geodataframes(main_geodataframe: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    
    # Srednia i mediana wartości pomiaru w podziale na daty w poszczegolnych wojewodztwach i powiatach:
    opady_woj = main_geodataframe.groupby(["name_woj", "date"])["value"].aggregate(["mean", "median"])
    opady_pow = main_geodataframe.groupby(["name_pow", "date"])["value"].aggregate(["mean", "median"])

    return opady_woj, opady_pow

def calculate_stat_change(gdf: gpd.GeoDataFrame, gov_unit_name: str, date_start: datetime, date_end: datetime):
    """
    Args:
        gdf (gpd.GeoDataFrame): result of create_geodataframes function GeoDataFrame containing gov_unit of interest
        date_start (datetime): Format: "yyyy-mm-dd HH:MM" - has to match date and time in gdf index
        date_end (datetime): Format: "yyyy-mm-dd HH:MM" 
    """
    date_format = "%Y-%m-%d %H:%M"
    time_start = datetime.strptime(date_start, date_format)
    time_end = datetime.strptime(date_end, date_format)
    
    xs = pd.IndexSlice
    try:
        mean_end = gdf.loc[xs[gov_unit_name, time_end], "mean"]
        mean_start = gdf.loc[xs[gov_unit_name, time_start], "mean"]
        
        median_end = gdf.loc[xs[gov_unit_name, time_end], "median"]
        median_start = gdf.loc[xs[gov_unit_name, time_start], "median"]
        
        print(f"***\nJednostka: {gov_unit_name}\nDaty: {date_start}, {date_end}\nRóżnica średniej: {mean_end - mean_start}\nRóżnica mediany: {median_end - median_start}")
        return mean_end - mean_start, median_end - median_start
    except:
        print("! | ERR: calc_stat_change")
        return
        
if __name__ == '__main__':
    
    print(create_wojewodztwa().iloc[0])
    # pass
    
    # d,m,a,b,c,x = create_main_dataframes()
    # day, ni, h = create_dataframes(m)
    # for i in day.index:
    #     print(day.loc[i]['mean'])
    
    # print(m.iloc[50])
    # e,f = create_geodataframes(d)
    # calculate_stat_change(e, 'mazowieckie', "2023-10-04 06:00", "2023-10-01 06:00")
    
    # print(a)
    # print()
    # print(b)
    
    # Wyciaganie danych
    
    # print(a.loc[xs[:,'2023-10-01 06:00:00'], :])