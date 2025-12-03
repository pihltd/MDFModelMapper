#Creates edges in a neo4j db

import bento_mdf
from crdclib import crdclib
import argparse
import os
import sys
import json
sys.path.insert(1,'../CRDCTransformationLibrary/src')
import mdfTools
import Neo4JConnection as njc
import cypherQueryBuilders as cqb

def main(args):
    if args.verbose >= 1:
        print(f"Reading configuration file {args.configfile}")
    configs = crdclib.readYAML(args.configfile)

    if args.verbose >= 1:
        print("Creatind MDF model object")
    mdf = bento_mdf.MDF(*configs['mdffiles'])

    if args.verbose >= 1:
        print("Establishing database connection")
    conn = njc.Neo4jConnection(os.getenv('NEO4J_URI'), os.getenv('NEO4J_USERNAME'),os.getenv('NEO4J_PASSWORD'))

    if args.verbose >= 1:
        print('Getting database labels')
    fullnodelist = cqb.cypherUniqueLabels(conn)
    dbnodelist = []
    for node in fullnodelist:
        if f"{configs['nodeprefix']}_" in node:
            dbnodelist.append(node)
    print(dbnodelist)


    if configs['modeledges']:
         # NOTE: The to_node (target model node) should be the MDF relationship src node. The key fields from the dst node should be added to the src node load sheet.
         # This should all be WITHIN a model, no intermodel connections
        for node in dbnodelist:
            print(f"Creating model edges for {node}")
            modelnode = node.replace(f"{configs['nodeprefix']}_", '').lower()
            srcedgelist = mdf.model.edges_by_src(mdf.model.nodes[modelnode])
            for edge in srcedgelist:
                dstnode = edge.dst.handle
                if 'nodeprefix' in configs:
                    newdstnode = f"{configs['nodeprefix'].lower()}_{dstnode}"
                srckeylist = mdfTools.getKeyProperty(node=dstnode, mdf=mdf)
                #print(f"Node {modelnode}\tDSTnode: {dstnode}\tSource key list: {srckeylist}")
                for srckey in srckeylist:
                    #edgequery = cqb.cypherRelationshipQuery(node, newdstnode, f"of_{modelnode}", srckey)
                    edgequery = cqb.cypherRelationshipQuery(node, dstnode, f"of_{modelnode}", srckey, configs['nodeprefix'])
                    print(edgequery)
                    conn.query(edgequery, db='neo4j')

    if configs['parentedges']:
        # Create links to parent nodes based on elid
        # Query for all instances of a node label
        # Get the elids for the node
        # 
        #dbnodelist = ['gc_diagnosis']
        for node in dbnodelist:
            print(f"Creating TRANSFORM_OF for {node}")
            modelnode = node.replace(f"{configs['nodeprefix']}_", '').lower()
            childquery = cqb.cypherGetNodeQuery(node)
            #print(childquery)
            childresults = conn.query(childquery, db='neo4j')
            for childresult in childresults:
                #print(childresult)
                parentelids = childresult[node.lower()]['parent_elementId'].replace("'", '"')
                childelid = childresult['elid']
                #print(parentelids)
                parentelids = json.loads(parentelids)
                for entry in parentelids:
                    for parentnode, parentelid in entry.items():
                        rellabel = f"TRANSFORM_OF_{parentnode.upper()}"
                        elidquery = cqb.cypherElementIDRelationshipQuery(parentnode, node, rellabel, parentelid, childelid )
                        #print(elidquery)
                        conn.query(elidquery, db='neo4j')


    


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument('-v', '--verbose', action='count', default=0, help=("Verbosity: -v main section -vv subroutine messages -vvv data returned shown"))

    args = parser.parse_args()

    main(args)

