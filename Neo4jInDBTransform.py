#Is it possible to use a destination model and pull data from a source model in the db.  No load sheet based transforms.
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import bento_mdf
import pandas as pd
#import numpy as np
import argparse
import os
from crdclib import crdclib
import sys
sys.path.insert(1,'../CRDCTransformationLibrary/src')
import mdfTools
import Neo4JConnection as njc
import cypherQueryBuilders as cqb
import GCNodeTransformations as gc

def result2DF(results, node):
    reslist = []
    for result in results:
        reslist.append(result[node])
    df = pd.DataFrame(reslist)
    return df
        


def dfDBClean(df, nodelist):
    #Removes any rows where ['lift_from_node'] is not in the database.  There's literally nothing to transfer
    dflist = []
    for node in nodelist:
        node=node.lower()
        node_df = df.query('lift_from_node == @node')
        dflist.append(node_df)
    return pd.concat(dflist)
        



def main(args):
    if args.verbose >= 1:
        print("Reading configs")
    configs = crdclib.readYAML(args.configfile)

    if args.verbose >= 1:
        print("Setting up source and destination data models")
    dstmdf = bento_mdf.MDF(*configs['dstmdffiles'])
    srcmdf = bento_mdf.MDF(*configs['srcmdffiles'])

    dsthandle = dstmdf.handle
    dstversion = dstmdf.version

    srchandle = srcmdf.handle
    srcversion = srcmdf.version

    if args.verbose >= 1:
        print("Reading mapping files")

    if args.verbose >= 1:
        print("Establishing db connection")
    conn = njc.Neo4jConnection(os.getenv('NEO4J_URI'), os.getenv('NEO4J_USERNAME'), os.getenv('NEO4J_PASSWORD'))

    if args.verbose >= 1:
        print("Getting source nodes")
    srcnodelist = cqb.cypherUniqueLabels(conn, srchandle, srcversion, 'neo4j')
    if args.verbose >= 2:
        print(f"Source node list: {srcnodelist}")
    
    # Get all the transformation info for the destination model
    dst_query = cqb.cypherDstTransformQuery('tranforminfo', dsthandle, dstversion, df=False)
    if args.verbose >= 1:
        print(dst_query)
    result = conn.query(query=dst_query)
    dstmap_df = result2DF(result, 'tranforminfo')
    if args.verbose >= 2:
        print(f"Mapping dataframe is {len(dstmap_df)} in db")
    dstmap_df = dfDBClean(dstmap_df, srcnodelist)
    if args.verbose >= 2:
        print(f"Mapping dataframe is {len(dstmap_df)} after cleanup")
        #print(dstmap_df.lift_to_node.mode().tolist()[0])

    dstnodelist = dstmap_df.lift_to_node.unique().tolist()
    if args.verbose >= 2:
        print(f"Dst node list: {dstnodelist}")

    for dstnode in dstnodelist:
        map_df = dstmap_df.query('lift_to_node == @dstnode')
        if args.verbose >= 2:
            print(f"\n{map_df}")
        srcnodelist = map_df.lift_from_node.unique().tolist()
        if args.verbose >= 2:
            print(f"Starting srcnodelist: {srcnodelist}")
        firstnode = map_df.lift_from_node.mode().tolist()[0]
        #print(f"Removing {firstnode} from srcnodelist and it is of type {type(firstnode)}")
        srcnodelist.remove(firstnode)
       # print(f"Srcnodelist is now {srcnodelist}")
        if args.verbose >= 2:
            print(f"Dst Node: {dstnode}\tFirst Node: {firstnode}\t Remaining list: {srcnodelist}")
        firstquery = cqb.cypherGetModelNodeQuery(firstnode, srchandle, srcversion)
        if args.verbose >= 1:
            print(firstquery)
        firstresults = conn.query(query=firstquery)
        for firstresult in firstresults:
            elids = []
            newnode = {}
            from_properties = firstresult[firstnode].keys()
            for from_prop in from_properties:
                if from_prop in map_df.lift_from_prop.unique().tolist():
                    to_prop_df = map_df.query('lift_from_prop == @from_prop')
                    for index, row in to_prop_df.iterrows():
                        newnode[row['lift_to_prop']] = firstresult[firstnode][row['lift_from_prop']]
            elids.append({firstnode:firstresult['elid']})
            # Before moving on to the next result, run through any other nodes to collect those
            for secondarynode in srcnodelist:
                secondaryquery = cqb.cypherGetModelNodeQuery(secondarynode, srchandle, srcversion)
                secondaryresults = conn.query(query=secondaryquery)
                for secondaryresult in secondaryresults:
                    secondary_from_properties = secondaryresult[secondarynode].keys()
                    for secondary_from_property in secondary_from_properties:
                        if secondary_from_property in map_df.lift_from_prop.unique().tolist():
                            secondary_prop_df = map_df.query('lift_from_prop == @secondary_from_property')
                            for sindex, srow in secondary_prop_df.iterrows():
                                newnode[srow['lift_to_prop']] = secondaryresult[secondarynode][row['lift_from_prop']]
                    elids.append({secondarynode: secondaryresult['elid']})
            newnode['parent_elementId'] = elids
            if args.verbose >= 3:
                print(newnode)


# For each dst node
# Create transform info df
# Create srcnode list from df
# Pick primary node to query
# Per result of primary node:
#   Load each property from result
#   Load elementID
#   Node specific tailoring (releationship fields, etc.)
#   Query each remaining nod in srcnodelist
#   Per result of secondary node
#     Load each property and elementID

    



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument('-v', '--verbose', action='count', default=0, help=("Verbosity: -v main section -vv subroutine messages -vvv data returned shown"))

    args = parser.parse_args()

    main(args)