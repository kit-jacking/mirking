# address: http://localhost:8081:6379
# 'ifcid', 'type', 'date', 'value', 'geometry', 'index_woj', 'name_woj', 'index_pow', 'name_pow'

import redis

from dataframeCreation import create_main_dataframe

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
a = r.keys("*:*:krosno:*:*")
print(a)
r.close()
