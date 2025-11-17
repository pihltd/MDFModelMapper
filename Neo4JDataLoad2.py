import neo4j
import bento_mdf
import pandas as pd
import argparse
import os
import sys
from crdclib import crdclib
sys.path.insert(1,'../CRDCTransformationLibrary/src')
import mdfTools
import Neo4JConnection as njc
import cypherQueryBuilders as cqb



def main(args):
    if args.verbose >= 1:
        print(f"Reading configs from {args.configfile}")
    configs = crdclib.readYAML(args.configfile)

    if args.verbose >= 1:
        print("Creating MDF object")
    mdf = bento_mdf.MDF(*configs['mdffiles'])

    if args.verbose >= 1:
        print("Creating neo4j connection")
    conn = njc.Neo4jConnection(os.getenv('NEO4J_URI'), os.getenv('NEO4J_USERNAME'), os.getenv('NEO4J_PASSWORD'))

    # Get the list of csv files to load into neo4j
    filelist = configs['sourcefiles']
    if configs['dataload']:
        if args.verbose >= 1:
            print("Starting data load")
        for entry in filelist:
            for node, filename in entry.items():
                propertylist = pd.read_csv(filename, sep=",").columns.to_list()
                keyprop = mdfTools.getKeyProperty(node, mdf)[0]
                query = cqb.cypherLoadCSVQuery(nodelabel=node, filename=filename, proplist=propertylist, keyprop=keyprop, separator='csv' )
                if args.verbose >= 2:
                    print(query)
                # Load data if there's an actual query
                if query is not None:
                    conn.query(query=query, db='neo4j')
    
    #Data is loaded, can now build relationships/edges
    if configs['edges']:
        if args.verbose >= 1:
            print("Starting model-based relationship addition")
        for entry in filelist:
            for srcnode in entry.keys():
                # Need a list of edges associated with the source node
                edgelist = mdf.model.edges_by_src(mdf.model.nodes[srcnode])
                for edge in edgelist:
                    dstnode = edge.dst.handle
                    keyproperty = mdfTools.getKeyProperty(dstnode, mdf)[0]
                    edgelabel = f"OF_{srcnode.upper()}"
                    edgequery = cqb.cypherRelationshipQuery(srcnode, dstnode, edgelabel, keyproperty)
                    if edgequery is not None:
                        if args.verbose >=2:
                            print(edgequery)
                        conn.query(query=edgequery, db='neo4j')
        # If requested, add any relationships specified in the configuration file.
        if 'manualedges' in configs:
            if args.verbose >= 1:
                print("Starting manual relationship addition")
            manualedges = configs['manualedges']
            for manualedge in manualedges:
                for mansrcnode, mandstnode in manualedge.items():
                    mankeyproperty = mdfTools.getKeyProperty(mandstnode, mdf)[0]
                    manedgelabel = f"OF_{mansrcnode.upper()}"
                    edgequery = cqb.cypherRelationshipQuery(mansrcnode, mandstnode, manedgelabel, mankeyproperty)
                    if edgequery is not None:
                        if args.verbose >= 2:
                            print(edgequery)
                        conn.query(query=edgequery, db='neo4j')
                    else:
                        print("Edgequery is none")




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument('-v', '--verbose', action='count', default=0, help=("Verbosity: -v main section -vv subroutine messages -vvv data returned shown"))

    args = parser.parse_args()

    main(args)