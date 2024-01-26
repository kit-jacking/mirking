import numpy as np
from neo4j import GraphDatabase
from dataframeCreation import create_main_dataframe

def add_wojewodztwa(session):
    gfd = create_wojewodztwa()
    for i in range(0,16):
        print(gfd['name'][i])
        query = 'CREATE (' + str(gfd['name'][i]).replace("-", "_").replace(" ", "_") + ':Wojewodztwo {nazwa:"' + str(gfd['name'][i]) + '"});'
        session.run(query)
        query = 'MATCH(a:Kraj) WHERE a.nazwa = "Polska" MATCH(b:Wojewodztwo) WHERE b.nazwa = "' + \
                str(gfd['name'][i]) + '" CREATE (b)-[:JEST_W]->(a)'
        session.run(query)

def add_pomiary_stacje_i_powiaty(session):
    gfd = create_main_dataframe()
    dane_w_stacji = []
    prev_ifcid = ''
    for i in range(0,len(gfd['ifcid'])):
        if str(gfd['ifcid'][i]) == prev_ifcid or i == 0:
            dane_w_stacji.append("Pomiar" + str(i) + ': {value:' + str(gfd['value'][i]) + ' , date: "' + str(
                gfd['date'][i]) + '", value:"' + str(gfd['value'][i]) + '},')
        elif str(gfd['ifcid'][i]) != prev_ifcid:
            query = 'CREATE (ID' + prev_ifcid + ' :Stacja {'
            for i in dane_w_stacji:
                query += i
            query += "IFCID: " + str(gfd['ifcid'][i])
            query += '"});'
            session.run(query)
            dane_w_stacji = []
            dane_w_stacji.append("Pomiar" + str(i) + ': {value:' + str(gfd['value'][i]) + ' , date: "' + str(
                gfd['date'][i]) + '", value:' + str(gfd['value'][i]) + '},')

        prev_ifcid = str(gfd['ifcid'][i])

def create_database():
    gfd = create_main_dataframe()
    for i in range(0, len(gfd['ifcid'])):
        query = "CREATE(Pomiar:ID" + str(i) + ' {value:' + str(gfd['value'][i]) + ' , date: "' + str(gfd['date'][i]) \
                + '", value:' + str(gfd['value'][i]) + '})'
        query += "-[:Przeprowadzony_W {type: '" + str(gfd['type'][i]) + "'} ]->"
        query += '(ID' + str(gfd['ifcid'][i]) + ':Stacja { nazwa:' + str(gfd['ifcid'][i]) + '})'
        session.run(query)
        #Person:Actor {name: 'Charlie Sheen'})-[: ACTED_IN {role: 'Bud Fox'}]->(wallStreet:Movie {title: 'Wall Street'}) < -[: DIRECTED]-(oliver:Person:Director {name: 'Oliver Stone'})"


def addPolska():
    query = "CREATE(Polska: Kraj {nazwa: 'Rzeczpospolita Polska', populacja: 38538447})"
    session.run(query)


def addWojewodztwa(gdf):
    woj = gdf['name_woj']
    woj = list(set(woj))
    for w in woj:
        query = "CREATE(" + w.replace("-", "_").replace(" ", "_") + ": Wojewodztwo {nazwa: '" + w + "'})"
        session.run(query)
        session.run('MATCH(c: Wojewodztwo {nazwa: "' + w + '"}), (v:Kraj {nazwa:"Rzeczpospolita Polska"}) CREATE(c) - [: JEST_W]->(v)')


def addPowiaty(gdf):
    pow = gdf['name_pow']
    pow = list(set(pow))
    for p in pow:
        query = "CREATE(" + p.replace("-", "_").replace(" ", "_") + ": Powiat {nazwa: '" + p + "'})"
        session.run(query)
    #for i in pow:
    #query =



def addStacje(gdf):
    sta = gdf['ifcid']
    sta = list(set(sta))
    for s in sta:
        query = "CREATE(" + 'Stacja' + str(s).replace("-", "_").replace(" ", "_") + ": Stacja {nazwa: '" + str(s) + "'})"
        session.run(query)


def connectingAdm(gdf):
    woj = gdf['name_woj']
    woj = list(set(woj))
    pow = gdf['name_pow']
    pow = list(set(pow))
    sta = gdf['ifcid']
    sta = list(set(sta))
    #stations to powiaty
    for i in sta:
        powiat = np.array(gdf[gdf['ifcid'] == i].iloc[[0]]['name_pow'])[0]
        query = 'MATCH(c: Stacja {nazwa: "'
        query += str(i)
        query += '"}), (v:Powiat {nazwa:"' + powiat
        query += '"}) CREATE(c) - [: JEST_W]->(v)'
        session.run(query)

    # wojewodztwa to powiaty
    for i in pow:
        wojewodztwo = gdf[gdf['name_pow'] == i].iloc[0]['name_woj']
        query = 'MATCH(c: Powiat {nazwa: "'
        query += str(i)
        query += '"}), (v:Wojewodztwo {nazwa:"' + wojewodztwo
        query += '"}) CREATE(c) - [: JEST_W]->(v)'
        session.run(query)


def addingAndConnectingPomiary(gdf):
    for ind in gdf.index:
        query = "CREATE(Pomiar" + str(ind) + ":Pomiar {data: '" + str(gdf['date'][ind]) + "', pomiar: "
        query += str(gdf['value'][ind]) + ", powiat: '" + str(gdf['name_pow'][ind]) + "', wojewodztwo: '" + str(gdf['name_woj'][ind]) + "'})"
        session.run(query)
        query = "MATCH(c: Pomiar {data: '" + str(gdf['date'][ind]) + "', pomiar: "
        query += str(gdf['value'][ind]) + "}"
        query += '), (v:Stacja {nazwa:"' + str(gdf['ifcid'][ind]) + '"}) CREATE(c) - [: DOKONANY_W]->(v)'
        session.run(query)
    #def connectingStacjePowiaty():

def get_mean_value_by_wojewodztwo(wojewodztwo):
    query = f'match (p:Powiat)-[:JEST_W]->(w:Wojewodztwo where w.nazwa="{wojewodztwo}") return p.nazwa;'
    powiaty = session.run(query).value()
    pomiary = []
    for i in powiaty:
        #print(i)
        pomiary.append(float(get_mean_value_by_powiat(i)))
    return np.mean(pomiary)



def get_mean_value_by_powiat(powiat):
    query = f'match (s:Stacja)-[:JEST_W]->(p:Powiat where p.nazwa="{powiat}") return s.nazwa;'
    stacje = session.run(query).value()
    pomiary = []
    for i in stacje:
        pomiary.append(float(get_mean_value_by_station(i)))
    return np.mean(pomiary)



def get_mean_value_by_station(station):
    query = f'match (p:Pomiar)-[: DOKONANY_W]->(s:Stacja where s.nazwa="{station}") return p.pomiar;'
    pomiary = session.run(query)
    pomiary = pomiary.value()
    pomiar = np.mean(pomiary)
    #print("Pomiar", pomiar)
    #print(pomiar)
    return str(pomiar)




if __name__ == "__main__":
    print("Connecting with database...")
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=('neo4j', 'kuba1234'))
    session = driver.session()

    """print("Deleting everything in database...")
    session.run("MATCH(n) DETACH DELETE n")
    gdf = create_main_dataframe()
    print("Adding Polska...")
    addPolska()
    print("Adding Wojewodztwa...")
    addWojewodztwa(gdf)
    print("Adding Powiaty...")
    addPowiaty(gdf)
    print("Adding Stacje...")
    addStacje(gdf)
    print("Connecting Administrative Nodes...")
    connectingAdm(gdf)
    print("Adding and Connecting Pomiary...")
    addingAndConnectingPomiary(gdf)"""


    value = get_mean_value_by_wojewodztwo('warmińsko-mazurskie')
    print(f"Mean value wojewodztwo warmińsko-mazurskie = {value}")

    value = get_mean_value_by_powiat("legionowski")
    print(f"Mean value powiat legionowski = {value}")


    session.close()
