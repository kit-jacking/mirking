from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=('neo4j', 'kuba1234'))
session = driver.session()
query = 'CREATE (m:Miasto {nazwa:"Warszawa"});'
session.run(query)

session.close()