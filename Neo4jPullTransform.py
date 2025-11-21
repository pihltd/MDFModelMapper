# Rather than push from CCDI to GC, can we pull to GC from CCDI.  In other words, we establishe the GC loading sheets and then pull based on the mappingimport warnings
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import bento_mdf
import pandas as pd
import numpy as np
import argparse
import os
from crdclib import crdclib
import sys
sys.path.insert(1,'../CRDCTransformationLibrary/src')
import mdfTools
import Neo4JConnection as njc
import cypherQueryBuilders as cqb



def singleNodeMappingLoad(from_node, to_df, to_mapping_df, conn):
    # Get the database entries
    query = cqb.cypherGetNodeQuery(from_node)
    results = conn.query(query=query)
    for result in results:
        #Each result is a line in the dataframe, set a new one
        loadline = {}
        from_properties = list(result[from_node.lower()].keys())
        for from_property in from_properties:
            if from_property in to_mapping_df['lift_from_prop'].unique().tolist():
                prop_df = to_mapping_df.query('lift_from_prop == @from_property')
                for index, row in prop_df.iterrows():
                    loadline[row['lift_to_prop']] = result[from_node][from_property]
                loadline['parent_elementId'] = result['elid']
        to_df.loc[len(to_df)] = loadline
    return to_df



def mulitNodeMappingLoad(to_node, to_df, to_mapping_df, conn):
    #This assumes that the from_node has the same name as one of the nodes in the to_nodes, but pulls from other nodes as well
    # Process steps:
    # 1: For the "main" node (to_node == from_node), query for all results
    # 2: For each result, fill in mapped properties
    # 2a: query remaining nodes, loop through results and fill in properties.
    mapped_node_df = to_mapping_df.query('lift_to_node == @to_node')
    from_node_list = mapped_node_df['lift_from_node'].unique().tolist()
    from_node_list.remove(to_node)
    #Query for the from_node info
    to_query = cqb.cypherGetNodeQuery(to_node)
    main_results = conn.query(query=to_query)
    for mainres in main_results:
        loadline = {}
        elids = []
        from_properties = list(mainres[to_node].keys())
        for from_property in from_properties:
            if from_property in to_mapping_df['lift_from_prop'].unique().tolist():
                prop_df = to_mapping_df.query('lift_from_prop == @from_property')
                for index, row in prop_df.iterrows():
                        loadline[row['lift_to_prop']] = mainres[to_node][from_property]
        elids.append(mainres['elid'])
        # At this point the main node should be done so time to fill in from the secondary nodes
        # BUT I'm not sure how to do this in a sane fasion.  
        for secnode in from_node_list:
            secquery = cqb.cypherGetNodeQuery(secnode)
            secresults = conn.query(query=secquery)
            for secres in secresults:
                secprops = 'UGH'


    '''loadline = {}
    for fnode in from_node_list:
        #loadline = {}
        #elids = []
        print(f"Working on {fnode} from {from_node_list}\nLoadline: {loadline}")
        query = cqb.cypherGetNodeQuery(fnode)
        results = conn.query(query=query)
        for result in results:
            elids = []
            from_properties = list(result[fnode.lower()].keys())
            for from_property in from_properties:
                if from_property in to_mapping_df['lift_from_prop'].unique().tolist():
                    prop_df = to_mapping_df.query('lift_from_prop == @from_property')
                    for index, row in prop_df.iterrows():
                        loadline[row['lift_to_prop']] = result[fnode][from_property]
            if result['elid'] not in elids:
                elids.append(result['elid'])
                loadline['parent_element_id'] = elids
            print(f"Loding line {loadline}")
            to_df.loc[len(to_df)] = loadline'''
    return to_df



def mappingTransform(to_node, to_df, to_mapping_df, conn):
    # Steps needed:
    # 1- Set up mapped properties list and mapped nodes list
    # 2- If there is only 1 node, query for all instances of that node
    # 2a - For each returned node, pull the data from the lift_from property
    # 2b - For each returned node, grab the parent elementId
    # 3- If there is more than one node
    #to_prop_list = list(to_df.columns)
    #mapped_prop_list = to_mapping_df['lift_to_prop'].unique().tolist()
    query_node_list = to_mapping_df['lift_from_node'].unique().tolist()
    print(f"To node: {to_node} maps to {len(query_node_list)} db nodes")
    if len(query_node_list) == 1:
        to_df = singleNodeMappingLoad(query_node_list[0], to_df, to_mapping_df, conn)
        #print(to_df)
    elif len(query_node_list) > 1:
        #if to_node in query_node_list:
        #    print(f"To Node {to_node} found in {query_node_list}")
        to_df = mulitNodeMappingLoad(to_node, to_df, to_mapping_df, conn)
        #print(to_df)
    else:
        print(f"To Node {to_node} NOT FOUND in {query_node_list}")
    return to_df
        
def addElementID(loadsheets_df):
    for node, loadsheet in loadsheets_df.items():
        columns = loadsheet.columns.tolist()
        columns.append('parent_elementId')
        temp_df = pd.DataFrame(columns=columns)
        loadsheets_df[node] = temp_df
    return loadsheets_df




def writeTransformedLoadsheets(dataframecollection, outputdir):

    for node, df in dataframecollection.items():
        filename = f"{outputdir}{node}_TRANSFORMED.csv"
        df.to_csv(filename, sep="\t", index=False)




def main(args):
    # Basic configureation and setup
    if args.verbose >= 1:
        print("Parsing configuration file")
    configs = crdclib.readYAML(args.configfile)

    if args.verbose >= 1:
        print("Reading transformation files")
    #transform_df is the full model-model mapping file
    transform_df = pd.read_csv(configs['transform_file'], sep="\t")
    to_node_list = transform_df['lift_to_node'].unique().tolist()

    if args.verbose >= 1:
        print("Building lift_to_ model")
    lift_to_mdf = bento_mdf.MDF(*configs['lift_to_model_files'])

    if args.verbose >= 1:
        print("Establishing database connection")
    conn = njc.Neo4jConnection(os.getenv('NEO4J_URI'), os.getenv('NEO4J_USERNAME'),os.getenv('NEO4J_PASSWORD'))

    # Get a list of nodes in the database
    fromnodelist = cqb.cypherUniqueLabels(dbconn=conn)
    if args.verbose >= 1:
        print(f"Labels found in database: {fromnodelist}")

    # Get the empty load sheets for the to_model
    temp_to_loadsheets = crdclib.mdfBuildLoadSheets(lift_to_mdf)
   
    

    # Drop any nodes that aren't in the database
    to_loadsheets = {}
    for key, value in temp_to_loadsheets.items():
        if key.upper() in fromnodelist:
            to_loadsheets[key] = value

     # Add a column for the parent elementId
    to_loadsheets = addElementID(to_loadsheets)

    for to_node, to_df in to_loadsheets.items():
        to_mapping_df = transform_df.query('lift_to_node == @to_node')
        to_df = mappingTransform(to_node, to_df, to_mapping_df, conn)
        to_loadsheets[to_node] = to_df
    
    writeTransformedLoadsheets(to_loadsheets, configs['outputdir'])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument('-v', '--verbose', action='count', default=0, help=("Verbosity: -v main section -vv subroutine messages -vvv data returned shown"))

    args = parser.parse_args()

    main(args)