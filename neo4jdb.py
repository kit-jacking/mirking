# http://localhost:7474/
# match (n) detach delete n
from neo4j import GraphDatabase
import numpy as np
from dataframeCreation import create_main_dataframe

# 'ifcid', 'type', 'date', 'value', 'geometry', 'index_woj', 'name_woj', 'index_pow', 'name_pow'

def renew_neo4j_sesion_database(session):
    stacje = create_main_dataframe()
    stacje["date"] = stacje["date"].astype(str)

    wojewodztwa = stacje["name_woj"].unique()
    powiaty = stacje["name_pow"].unique()
    id_stacji = stacje["ifcid"].unique()

    session.run("match (n) detach delete n")

    session.run('create (temp:kraj {nazwa:"Polska"})')
    print("Wojewodztwa")
    for i, wojewodztwo in enumerate(wojewodztwa):
        print(f"{i}. creating {wojewodztwo}")
        session.run('CREATE (temp:woj {nazwa:"' + wojewodztwo + '"})')

    print("Powiaty")
    for i, powiat in enumerate(powiaty):
        print(f"{i}. creating {powiat}")
        session.run('CREATE (temp:pow {nazwa:"' + powiat + '"})')

    print("Stacje")
    for i, stacja in enumerate(id_stacji):
        print(f"{i}. creating {stacja}")
        session.run('CREATE (temp:stacja {nazwa:"' + str(stacja) + '"})')

    for i, powiat in enumerate(powiaty):
        print(f"{i}. connecting {powiat}")
        woj = stacje[stacje["name_pow"] == powiat].iloc[0]["name_woj"]
        session.run('MATCH (w:woj) where w.nazwa="' + woj + '"'
                                                            'MATCH (p:pow) where p.nazwa="' + powiat + '"'
                                                                                                       'CREATE (p)-[:JEST_W]->(w);')

    for i, id_stacja in enumerate(id_stacji):
        print(f"{i}. connecting {id_stacja}")
        powiat = stacje[stacje["ifcid"] == id_stacja].iloc[0]["name_pow"]
        session.run('MATCH (p:pow) where p.nazwa="' + powiat + '"'
                                                               'MATCH (s:stacja) where s.nazwa="' + str(id_stacja) + '"'
                                                                                                                     'CREATE (s)-[:JEST_W]->(p);')
    for i, pomiar in stacje.iterrows():
        if not i % 100: print(f"{i}. connecting station {pomiar['ifcid']} date {pomiar['date']}")
        id_stacja = pomiar["ifcid"]
        data = pomiar["date"]
        wartosc = pomiar["value"]

        query = 'MATCH (s:stacja where s.nazwa="' + str(
            id_stacja) + '") CREATE (o:pomiar {data: "' + data + '", wartosc: ' + str(
            wartosc) + ' }), (o)-[:ZROBIONY_W]->(s)'
        session.run(query)

def get_mean_value_by_voivodeship(session, voivodeship):
    query = f'match (p:pow)-[:JEST_W]->(w:woj where w.nazwa="{voivodeship}") return p.nazwa;'
    powiaty = session.run(query).value()
    srednie = [get_mean_value_by_powiat(session, powiat) for powiat in powiaty]
    return np.mean(srednie)

def get_mean_value_by_powiat(session, powiat):
    query = f'match (s:stacja)-[:JEST_W]->(p:pow where p.nazwa="{powiat}") return s.nazwa;'
    stacje = session.run(query).value()
    srednie = [get_mean_value_by_station(session, stacja) for stacja in stacje]
    return np.mean(srednie)

def get_mean_value_by_station(session, station):
    query = f'match (p:pomiar)-[:ZROBIONY_W]->(s:stacja where s.nazwa="{station}") return p.wartosc;'
    pomiary = session.run(query)
    return np.mean(pomiary.value())

user = "neo4j"
password = "password"
driver = GraphDatabase.driver("bolt://localhost:7687", auth=(user, password))
session = driver.session()

przykladowe_wojewodztwo = "warmińsko-mazurskie"
przykladowy_powiat = "legionowski"
przykladowa_stacja = "250170390"

value = get_mean_value_by_voivodeship(session, przykladowe_wojewodztwo)
print(f"Mean value wojewodztwo {przykladowe_wojewodztwo}= {value}")

value = get_mean_value_by_powiat(session, przykladowy_powiat)
print(f"Mean value powiat {przykladowy_powiat} = {value}")

value = get_mean_value_by_station(session, przykladowa_stacja)
print(f"Mean value stacja {przykladowa_stacja} = {value}")

session.close()

# MATCH (s:staja where s.nazwa="250180590") CREATE (o:pomiar {data: "2023-09-01 06:00:00", wartosc: 0.0 }), (o)-[:ZROBIONY_W]->(s)
