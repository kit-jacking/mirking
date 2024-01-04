# http://localhost:7474/
# match (n) detach delete n
from neo4j import GraphDatabase
from dataframeCreation import create_main_dataframe

user = "neo4j"
password = "password"

# 'ifcid', 'type', 'date', 'value', 'geometry', 'index_woj', 'name_woj', 'index_pow', 'name_pow'
stacje = create_main_dataframe()
stacje["date"] = stacje["date"].astype(str)

wojewodztwa = stacje["name_woj"].unique()
powiaty = stacje["name_pow"].unique()
id_stacji = stacje["ifcid"].unique()

driver = GraphDatabase.driver("bolt://localhost:7687", auth=(user, password))
session = driver.session()

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
    session.run('CREATE (temp:satcja {nazwa:"' + str(stacja) + '"})')

for i, powiat in enumerate(powiaty):
    print(f"{i}. connecting {powiat}")
    woj = stacje[stacje["name_pow"] == powiat].iloc[0]["name_woj"]
    session.run('MATCH (w:woj) where w.nazwa="' + woj + '"'
                'MATCH (p:pow) where p.nazwa="' + powiat + '"'
                'CREATE (p)-[:JEST_W]->(w);')

session.close()
