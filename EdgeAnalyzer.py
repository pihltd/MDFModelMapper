#Creates edges in a neo4j db

import bento_mdf
from crdclib import crdclib
import argparse
import os
import sys
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
        for node in dbnodelist:
            modelnode = node.replace(f"{configs['nodeprefix']}_", '').lower()
            srcedgelist = mdf.model.edges_by_src(mdf.model.nodes[modelnode])
            for edge in srcedgelist:
                dstnode = edge.dst.handle
                if 'nodeprefix' in configs:
                    newdstnode = f"{configs['nodeprefix'].lower()}_{dstnode}"
                srckeylist = mdfTools.getKeyProperty(node=dstnode, mdf=mdf)
                print(f"Node {modelnode}\tDSTnode: {newdstnode}\tSource key list: {srckeylist}")
                for srckey in srckeylist:
                    edgequery = cqb.cypherRelationshipQuery(node, newdstnode, f"of_{modelnode}", srckey)
                    print(edgequery)
            #dstedgelist = mdf.model.edges_by_dst(mdf.model.nodes[modelnode])
            #print(f"Node: {modelnode}\t Edges by Src: {srcedgelist}\t Edges by Dst: {dstedgelist}")

    #if configs['parentedges']:
        # Create links to parent nodes based on elid

    


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument('-v', '--verbose', action='count', default=0, help=("Verbosity: -v main section -vv subroutine messages -vvv data returned shown"))

    args = parser.parse_args()

    main(args)

