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
    fullnodelist = cqb.cypherUniqueLabels(conn, mdf.handle, mdf.version)


    if configs['modeledges']:
         # NOTE: The to_node (target model node) should be the MDF relationship src node. The key fields from the dst node should be added to the src node load sheet.
         # This should all be WITHIN a model, no intermodel connections
        for node in fullnodelist:
            node = node.lower()
            if args.verbose >= 1:
                print(f"Creating model edges for {node}")
            srcedgelist = mdf.model.edges_by_src(mdf.model.nodes[node])
            for edge in srcedgelist:
                dstnode = edge.dst.handle
                srckeylist = mdfTools.getKeyProperty(node=dstnode, mdf=mdf)
                for srckey in srckeylist:
                    edgequery = cqb.cypherModelRelationshipQuery(node, dstnode, f"of_{node}", srckey, mdf.handle, mdf.version)
                    if args.verbose >= 2:
                        print(edgequery)
                    conn.query(edgequery, db='neo4j')

    if configs['cleanup']:
        for node, proplist in configs['cleannodes'].items():
            #rint(f"Clean node {node} proplist {proplist}")
            query = cqb.cypherDeleteFileDuplicateEdgesQuery(node, proplist[0], proplist[1], mdf.handle, mdf.version)
            conn.query(query=query, db='neo4j')

    if configs['parentedges']:
        # Create links to parent nodes based on elid
        # Query for all instances of a node label
        # Get the elids for the node
        # 
        for node in fullnodelist:
            node = node.lower()
            if args.verbose >= 1:
                print(f"Creating TRANSFORM_OF for {node}")
            childquery = cqb.cypherGetModelNodeQuery(node, mdf.handle, mdf.version)
            if args.verbose >= 2:
                print(childquery)
            childresults = conn.query(childquery, db='neo4j')
            for childresult in childresults:
                if args.verbose >=3:
                    print(childresult)
                    print(childresult[node.lower()]['parent_elementId'])
                parentelids = childresult[node.lower()]['parent_elementId'].replace("'", '"')
                childelid = childresult['elid']
                if args.verbose >=3:
                    print(parentelids)
                parentelids = json.loads(parentelids)
                for entry in parentelids:
                    for parentnode, parentelid in entry.items():
                        rellabel = f"TRANSFORM_OF_{parentnode.upper()}"
                        elidquery = cqb.cypherElementIDRelationshipQuery(parentnode, node, rellabel, parentelid, childelid)
                        if args.verbose >=2:
                            print(elidquery)
                        conn.query(elidquery, db='neo4j')
    if configs['customedges']:
        print("doing custom edges")


    


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument('-v', '--verbose', action='count', default=0, help=("Verbosity: -v main section -vv subroutine messages -vvv data returned shown"))

    args = parser.parse_args()

    main(args)

