from neo4j import GraphDatabase
from dataframeCreation import create_wojewodztwa, create_powiaty

def add_wojewodztwa(session):
    gfd = create_wojewodztwa()
    for i in range(0,16):
        print(gfd['name'][i])
        query = 'CREATE (' + str(gfd['name'][i]).replace("-", "_") + ':Wojewodztwo {nazwa:"' + str(gfd['name'][i]) + '"});'
        session.run(query)
        query = 'MATCH(a:Kraj) WHERE a.nazwa = "Polska" MATCH(b:Wojewodztwo) WHERE b.nazwa = "' + \
                str(gfd['name'][i]) + '" CREATE (b)-[:JEST_W]->(a)'
        session.run(query)

def add_powiaty(session):
    gfd = create_powiaty()
    for i in range(0,len(gfd['name'])):
        print(gfd['name'][i])
        query = 'CREATE (' + str(gfd['name'][i]).replace("-", "_") + ':Powiat {nazwa:"' + str(gfd['name'][i]) + '"});'
        session.run(query)
        if gfd['name'][i][:2]:
        if gfd['name'][i][:2]:
        if gfd['name'][i][:2]:
        if gfd['name'][i][:2]:
        if gfd['name'][i][:2]:
        if gfd['name'][i][:2]:
        if gfd['name'][i][:2]:

        query = 'MATCH(a:Kraj) WHERE a.nazwa = "Polska" MATCH(b:Wojewodztwo) WHERE b.nazwa = "' + \
                str(gfd['name'][i]) + '" CREATE (b)-[:JEST_W]->(a)'
        session.run(query)

'''
if __name__ == "__main__":
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=('neo4j', 'kuba1234'))
    session = driver.session()
    query = 'CREATE (Polska:Kraj {nazwa:"Polska", populacja:37698294});'
    session.run(query)
    add_wojewodztwa(session)
    session.close()
