import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd

powiaty = gpd.read_file("dane/powiaty.shp").to_crs(epsg=2180)
woj = gpd.read_file("dane/woj.shp").to_crs(epsg=2180)

effacility = gpd.read_file("dane/effacility.geojson")
effacility.crs = 2180
opady_dzienne = pd.read_csv("dane/Meteo_2023-09/B00604S_2023_09.csv",
                            header=None,
                            delimiter=";").rename(columns={0: "ifcid", 1: "type", 2: "date", 3: "value"})
opady_dzienne['value'] = opady_dzienne['value'].str.replace(',', '.').astype(float)

statystyki_stacji = opady_dzienne.groupby(["ifcid"], as_index=False)["value"].describe()
stacje_zlaczone = statystyki_stacji.merge(effacility[["ifcid", "geometry"]], how="left", on="ifcid")
stacje_zlaczone = gpd.GeoDataFrame(stacje_zlaczone, geometry=stacje_zlaczone["geometry"]).to_crs(epsg=2180)

stacje_zlaczone = gpd.tools.sjoin(stacje_zlaczone, woj[["geometry", "name"]], how="left", rsuffix="woj")
stacje_zlaczone = gpd.tools.sjoin(stacje_zlaczone, powiaty[["geometry", "name"]], how="left", rsuffix="pow")
print(stacje_zlaczone.columns)

# stacje_zlaczone.plot()
# plt.show()
