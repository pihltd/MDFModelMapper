import sys
import os
import neo4j
sys.path.insert(1,'../CRDCTransformationLibrary/src')
import mdfTools
import Neo4JConnection as njc
import cypherQueryBuilders as cqb



testnode = 'sample'

conn = conn = njc.Neo4jConnection(os.getenv('NEO4J_URI'), os.getenv('NEO4J_USERNAME'),os.getenv('NEO4J_PASSWORD'))

elid = '4:aeadf63d-711d-4bb3-8a32-91ea3b539d56:28'
query = cqb.cypherElementIDQuery(elid)

results = conn.query(query=query, db='neo4j')
print(results[0]['s'])
print("\n")
print(results[0]['s'][''])

#countquery = cqb.cypherRecordCount(testnode)
#countquery = cqb.cypherGetBasicNodeQuery(testnode)
#countquery = cqb.cypherGetNodeQuery(testnode)
#query = "MATCH (s) WHERE elementId(s) = '4:aeadf63d-711d-4bb3-8a32-91ea3b539d56:0' RETURN s"
#countres = conn.query(query=query, db='neo4j')

#print(f"Result is of type {type(countres)}")
#for item in countres:
#    print(f"Item type: {type(item)}")
#    print(f"Record keys: {item.keys()}")
    #print(f"Record data: {item[testnode]}")
    #print(f"Node keys: {item[testnode].keys()}")
    #print(f"ElementId: {item['elid']}")
    #print(f"Record values: {item.values()}")
    #print(f"Specific item type: {type(item[testnode])}")
    #print(f"Node keys: {item[testnode].keys()}")
    #print(f"Record data: {item[testnode]}")

#print(f"Raw result:\t{countres}")
#for item in neo4j.Result.__iter__(countres):
 #   print(f"Item: {item}\n")

#print(f"Keys result: {countres.keys()}")