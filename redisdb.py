# address: http://localhost:8081:6379
# 'ifcid', 'type', 'date', 'value', 'geometry', 'index_woj', 'name_woj', 'index_pow', 'name_pow'

import redis
import statistics
from dataframeCreation import create_main_dataframe

def connect_to_db(host: str, port: str, password: str):
    pool = redis.ConnectionPool(host = host, port = port, password = password)
    db = redis.Redis(connection_pool=pool)
    return db


def create_redis_db(connection):
    stacje = create_main_dataframe()
    stacje["date"] = stacje["date"].astype(str)

    for id_stacji in stacje["ifcid"].unique():
        stacja = stacje[stacje["ifcid"] == id_stacji]
        wiersz = stacja.iloc[0]

        # Klucz wygląda tak: id_stacji:wojewozdztwo:powiat:wspolrzedna_x:wspolrzedna_y
        klucz = f'{str(wiersz["ifcid"])}:{str(wiersz["name_woj"])}:{str(wiersz["name_pow"])}:{str(wiersz["geometry"].coords[0][0])}:{str(wiersz["geometry"].coords[0][1])}'.lower()

        wartosc = dict()
        for i, row in stacja.iterrows():
            wartosc[row["date"]] = row["value"]

        connection.hset(klucz, mapping=wartosc)

        print(f"Przerobiło: {klucz}")


r = redis.Redis(host='localhost', port=8081, decode_responses=True)

# create_redis_db(r)

# a = r.keys("*")
# for key in a:
#     id, wojewodztwo, powiat, x, y = key.split(":")
#     print(wojewodztwo)

nazwa_wojewodztwa = "warmińsko-mazurskie"
wojewodztwo_keys = r.keys(f"*:{nazwa_wojewodztwa}:*:*:*")
wartosci = []
for key in wojewodztwo_keys:
    wartosci += [float(x) for x in list(r.hgetall(key).values())]

output = statistics.mean(wartosci)
print(f"Średni opad przez cały wrzesień w województwie {nazwa_wojewodztwa[:-1]}m wyniósł: {output}")
r.close()
