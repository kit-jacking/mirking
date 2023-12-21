import geopandas as gpd
import pandas as pd

import pymongo
import sys
import json



# *** CONNECT TO MONGO DB
mongo_credentials = input("Enter MongoDB credentials link: ")


# Check if creds are correct
print('Connecting to Mongo...')
try:
    client = pymongo.MongoClient(mongo_credentials) 
except pymongo.errors.ConfigurationError:
    print("An Invalid URI host error was received. Is your Atlas host name correct in your connection string?")
    sys.exit(1)
    

# Load powiaty, wojewodztwa and effacilities from files
print('Initializing geodataframes...')
powiaty = gpd.read_file(r"C:\Users\qattr\Desktop\STUD\SEM 5\PAG\Projekt-2\Dane\powiaty.shp").to_crs(epsg=2180)
woj = gpd.read_file(r"C:\Users\qattr\Desktop\STUD\SEM 5\PAG\Projekt-2\Dane\woj.shp").to_crs(epsg=2180)
effacility = gpd.read_file(r"C:\Users\qattr\Desktop\STUD\SEM 5\PAG\Projekt-2\Dane\effacility.geojson").set_crs('epsg:2180', allow_override=True) 




# *** IMGW DATA GDF CREATION
print('Reading "opady"...')
opady_dzienne = pd.read_csv(rf"C:\Users\qattr\Desktop\STUD\SEM 5\PAG\Projekt-2\Dane\Dane-IMGW\{year}-{month}\B00604S_{year}_{month}.csv",
                            header=None,
                            delimiter=";").rename(columns={0: "ifcid", 1: "type", 2: "date", 3: "value"})
opady_dzienne['value'] = opady_dzienne['value'].str.replace(',', '.').astype(float)

stacje_zlaczone = opady_dzienne.merge(effacility[["ifcid", "geometry"]], how="left", on="ifcid")
stacje_zlaczone = gpd.GeoDataFrame(stacje_zlaczone, geometry=stacje_zlaczone["geometry"]).to_crs(epsg=2180)

stacje_zlaczone = gpd.tools.sjoin(stacje_zlaczone, woj[["geometry", "name"]], how="left", rsuffix="woj")
stacje_zlaczone = gpd.tools.sjoin(stacje_zlaczone, powiaty[["geometry", "name"]], how="left", rsuffix="pow").rename(
    columns={"name_left": "name_woj"})

# Delete NaN column 
del stacje_zlaczone[4]

stacje_zlaczone_json = json.loads(stacje_zlaczone.to_json(na='drop', to_wgs84=True))['features']

print(stacje_zlaczone.iloc[0])

# *** INSERT OF RAW DATA
# Insert data to daneIMGW db (for non-modified data)
print("Inserting data to db...")
db = client.daneIMGW
db_collection = db['opady']

# Drop collection if exists
try:
    db_collection.drop()
except pymongo.errors.OperationFailure:
    print("An authentication error was received. Are your username and password correct in your connection string?")
    sys.exit(1)

try:
    result = db_collection.insert_many(stacje_zlaczone_json)
except pymongo.errors.OperationFailure:
    print("An authentication error was received. Are you sure your database user is authorized to perform write operations?")
    sys.exit(1)

# *** CREATE DATABASES
db_woj = client['wojewodztwa']
db_powiat = client['powiaty']
db_stacja = client['stacje']
db_dzien = client['dni']
db_noc = client['noce']
db_doba = client['doby']

for r in range(len(stacje_zlaczone.index)):
    record = stacje_zlaczone.iloc[r]
    # Trzeba poprawic coordynaty, bo sie dziwnie dodaja + zmienic crs ; sprawdzic czy nie jest geom.geom_type NoneType; przerobic, zeby bylo insert many, bo straasznie wolno dziala, a ja nie lubie wolno
    geom = record['geometry']
    json_to_add = {'date': record['date'],'type': record['type'],'value': record['value'], 'geometry': {'type': geom.geom_type, 'coords': list(geom.coords)}}
    db_woj[record['name_woj']].insert_one(json_to_add)
# *** MODIFICATION OF DATA STRUCTURE

# # Create wojewodztwa db with info 
# # db = client['wojewodztwa']

# for w in woj['name']:
#     db_collection = db[w]
#     db_collection.insert_one({"1": "abc"})
# # print(powiaty['name'][0])


    
# result = db_collection.find({"properties.name_woj": "śląskie"})

# if result:
#     print(result[0])