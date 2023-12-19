from urllib import request
from zipfile import ZipFile
from os import remove

def download_data(year: int, month: int, save_dir: str):
    url = f'https://dane.imgw.pl/datastore/getfiledown/Arch/Telemetria/Meteo/{year}/Meteo_{year}-{month}.zip'
    file_name = f"Dane-IMGW-{year}-{month}.zip"
    request.urlretrieve(url, file_name)
    with ZipFile(file_name, 'r') as f:
        f.extractall(rf"{save_dir}\{year}-{month}")
    try:
        remove(file_name)
    except:
        print("Error occured while trying to remove zip file")