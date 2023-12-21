# address: http://localhost:8081:6379
# 'ifcid', 'type', 'date', 'value', 'geometry', 'index_woj', 'name_woj', 'index_pow', 'name_pow'

import redis
import statistics
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

def srednie_opady(connection) -> None:
    lista_nazw_wojewodztw =["dolnośląskie", "kujawsko-pomorskie", "lubelskie", "lubuskie", "łódzkie", "małopolskie", "mazowieckie",
                            "opolskie", "podkarpackie", "podlaskie", "pomorskie", "śląskie", "świętokrzyskie", "warmińsko-mazurskie", "wielkopolskie", "zachodniopomorskie"]

    for i, nazwa_wojewodztwa in enumerate(lista_nazw_wojewodztw):
        wojewodztwo_keys = connection.keys(f"*:{nazwa_wojewodztwa}:*:*:*")

        wartosci = []
        for key in wojewodztwo_keys:
            wartosci += [float(x) for x in list(connection.hgetall(key).values())]

        output = statistics.mean(wartosci)
        print(f"{i}. Średni opad przez cały wrzesień w województwie {nazwa_wojewodztwa[:-1]}m wyniósł: {output} mm")

r = redis.Redis(host='localhost', port=8081, decode_responses=True)

# create_redis_db(r)

# a = r.keys("*")
# for key in a:
#     id, wojewodztwo, powiat, x, y = key.split(":")
#     print(wojewodztwo)

srednie_opady(r)

r.close()
