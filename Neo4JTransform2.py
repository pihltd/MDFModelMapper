import neo4j
import bento_mdf
import pandas as pd
import numpy as np
import argparse
import os
from crdclib import crdclib
import sys
import logging
import uuid
sys.path.insert(1,'../CRDCTransformationLibrary/src')
import mdfTools
import Neo4JConnection as njc
import cypherQueryBuilders as cqb




def buildTransformedDataframes(transformed_df):
    dataframecollection = {}
    tonodelist = transformed_df['lift_to_node'].unique().tolist()
    for tonode in tonodelist:
        prop_df = transformed_df.query('lift_to_node == @tonode')
        toproplist = prop_df['lift_to_prop'].unique().tolist()
        toproplist.append('parent_elementId')
        to_df = pd.DataFrame(columns=toproplist)
        dataframecollection[tonode] = to_df
    return dataframecollection

def transformedLoadSheets(fromnode, queryresults, mapped_df, dataframecollection, verbose=0 ):
    # Get a list of mapped properties for the node (mapped_df is already node-specific)
    mappedPropertyList = mapped_df['lift_from_prop'].unique().tolist()
    if verbose >= 2:
        print(f"For node {fromnode}\t Mapped properties: {mappedPropertyList}")

    # Loop through the results, start with getting the node properties:data (fromdata) and the elementId (elid)
    for result in queryresults:
        # movingstuff holds the moved data
        movingstuff = {}
        fromdata = result.data()[fromnode.lower()]
        elementId = result.data()['elid']
        for property, value in fromdata.items():
            if property in mappedPropertyList:
                # The property is mapped, we need the rows specific to that property
                to_prop_df = mapped_df.query('lift_to_prop == @property')
                for index, row in to_prop_df.iterrows():
                    if row['lift_to_node'] in movingstuff.keys():
                        temp = movingstuff[row['lift_to_node']]
                        temp[property] = value
                        temp['parent_elementId'] = elementId
                        movingstuff[row['lift_to_node']] = temp
                    else:
                        temp ={}
                        temp[property] = value
                        temp['parent_elementId'] = elementId
                        movingstuff[row['lift_to_node']] = temp
        # Now move it all to dataframes
        for node, propstuff in movingstuff.items():
            if node in dataframecollection:
                temp_df = dataframecollection[node]
                temp_df.loc[len(temp_df)] = propstuff
                dataframecollection[node] = temp_df
            else:
                temp_df = pd.DataFrame(columns=list(propstuff.keys()))
                temp_df.loc[len(temp_df)] = propstuff
                dataframecollection[node] = temp_df
    return dataframecollection


def writeTransformedLoadsheets(dataframecollection, outputdir):

    for node, df in dataframecollection.items():
        filename = f"{outputdir}{node}_TRANSFORMED.csv"
        df.to_csv(filename, sep="\t", index=False)


def buildTransformedRelationships(mdf, nodelist, conn):
    for to_node in nodelist:
        edgelist = mdf.model.edges_by_src(mdf.model.nodes[to_node])
        edgenames = []
        for edge in edgelist:
            edgenames.append(edge.dst.handle)
        for dstnode in edgenames:
            if dstnode in nodelist:
                dataquery = cqb.cypherGetNodeQuery(dstnode)
                datares = conn.query(query=dataquery, db='neo4j')



def addEdgeKeys(dataframecollections, to_mdf):
    nodelist = list(dataframecollections.keys())
    for srcnode in nodelist:
        temp_df = dataframecollections[srcnode]
        edgelist = to_mdf.model.edges_by_src(to_mdf.model.nodes[srcnode])
        edgenames = []
        for edge in edgelist:
            edgenames.append(edge.dst.handle)
        for dstnode in edgenames:
            #print(f"Desitination Node {dstnode}")
            keylist = mdfTools.getKeyProperty(node=dstnode, mdf=to_mdf)
            for key in keylist:
                if f"{dstnode}.{key}" not in temp_df.columns.tolist():
                    temp_df.insert(0, f"{dstnode}.{key}", np.nan )
        dataframecollections[srcnode] = temp_df
    return dataframecollections



def populateEdgeKeys(dataframecollections, transform_df, conn):

    #Need a list of the unique nodes in the database:
    dbnodes = cqb.cypherUniqueLabels(conn)
    datacontainingnodes = list(dataframecollections.keys())
    print(f"Existing DB Nodes: {dbnodes}\nExisting Data Containing nodes: {datacontainingnodes}")
    for srcnode, data_df in dataframecollections.items():
        print(f"Working on srcnode {srcnode}")
        # Get the foreign key nodes
        foreignkeys = []
        for column in data_df.columns.to_list():
            if "." in column:
                foreignkeys.append(column)
        print(f"All foreign keys: {foreignkeys}")
        for fkey in foreignkeys:
            templist = fkey.split(".")
            fkeynode = templist[0]
            fkeyprop = templist[1]
            # Need to query the ORIGINAL node for data so get that from the transform db
            tempnode_df = transform_df.query('lift_to_node == @fkeynode')
            tempprop_df = tempnode_df.query('lift_to_prop == @fkeyprop')
            #print(f"Node filtered mapping dataframe:\n{tempnode_df}")
            #print(f"Prop filtered mapping dataframe:\n{tempprop_df}")
            #print(f"FKeynode: {fkeynode}\tFkeyprop: {fkeyprop}")
            for transformedindex, transformedrow in tempprop_df.iterrows():
                # Transformedrow should have the original node in lift_from_node
                print(f"Srcnode:{srcnode}\tFKeynode: {fkeynode}\tFkeyprop: {fkeyprop}\tLift_from_node: {transformedrow['lift_from_node']}")
                if transformedrow['lift_from_node'].upper() in dbnodes:
                    # Get the query 
                    #fkquery = cqb.cypherGetBasicNodeQuery(transformedrow['lift_from_node'])
                    countquery = cqb.cypherRecordCount(transformedrow['lift_from_node'])
                    #print(fkquery)
                    countres = conn.query(query=countquery, db='neo4j')
                    resultcount = countres[0]['count']
                    print(f"Node being counted: {transformedrow['lift_from_node']}\nResult: {resultcount}")
                    # If the row count is 1, it's likely we need to get and store the result to update multiple nodes
                    if resultcount == 1:
                        singlequery = cqb.cypherGetBasicNodeQuery(transformedrow['lift_from_node'])
                        singleres = conn.query(query=singlequery, db='neo4j')
                        for result in singleres:
                            #This should be a result object
                            noderes = result[transformedrow['lift_from_node']]
                            # And this should be a node object and lift_from_prop should have the value we need
                            propvalue = noderes[transformedrow['lift_from_prop']]
                            #print(f"Single property {transformedrow['lift_from_prop']} has value {propvalue}")
                            # So now we need to go and populated that single value into all the destination nodes
                            #data_df.update(pd.DataFrame({fkey:propvalue}))
                            for index, row in data_df.iterrows():
                                row[fkey] = propvalue
                                data_df.loc[index] = row
                            dataframecollections[srcnode] = data_df
                    #elif resultcount >= 2:
                        #print("Multiple results")
                        # The assumption here is that we need information from a specific node
                    #    for index, row in data_df.iterrows():
                    #        idquery = cqb.cypherElementIDQuery(transformedrow['lift_from_node'], row['parent_elementId'])
                            #idresults = conn.query(query=idquery, db='neo4j')
                            #for idresult in idresults:
                            #    idnoderes = idresult[transformedrow['lift_from_node']]
                            #    #idpropvalue = idnoderes[transformedrow['lift_from_prop']]
                            #    idpropvalue = idnoderes[fkey]
                            #    row[fkey] = idpropvalue
                            #    data_df.loc[index] = row
                    #    dataframecollections[srcnode] = data_df


    return dataframecollections



def main (args):

    # Steps:
    # 1: Basic config  and setup
    # 2: For each existing node (from node) get the transformations from the transformation file
    # 3: Using the node-specific transformation, move the mapped properties to a lift_to node.  Also add the lift_from elementId
    # 4: Create a transformed node-specific csv load sheet with the mapped properties:values 


# Basic configureation and setup
    if args.verbose >= 1:
        print("Parsing configuration file")
    configs = crdclib.readYAML(args.configfile)

    if args.verbose >= 1:
        print("Reading transformation files")
    transform_df = pd.read_csv(configs['transform_file'], sep="\t")

    if args.verbose >= 1:
        print("Establishing database connection")
    conn = njc.Neo4jConnection(os.getenv('NEO4J_URI'), os.getenv('NEO4J_USERNAME'),os.getenv('NEO4J_PASSWORD'))

    # Get a list of nodes in the database
    fromnodelist = cqb.cypherUniqueLabels(dbconn=conn)
    if args.verbose >= 1:
        print(f"Labels found in database: {fromnodelist}")

    # Step 2: Looping through existing nodes and getting the mappings for that node
    dataframecollections = {}
    if args.verbose >= 1:
        print("Creating from node transformation dataframes")
    for fromnode in fromnodelist:
        if args.verbose >= 2:
            print(f"Transformation dataframe for node {fromnode}")
        fromnode_df = transform_df.query('lift_from_node.str.upper() == @fromnode')

        #Step 3:
        if args.verbose >= 2:
            print(f"Querying Database for node {fromnode}")
        nodequery = cqb.cypherGetNodeQuery(fromnode)
        noderes = conn.query(query=nodequery)

        #Step 4: Create the load sheets for the transformed node
        if args.verbose >= 2:
            print(f"Adding {fromnode} to dataframe collection")
        dataframecollections = transformedLoadSheets(fromnode=fromnode, queryresults=noderes, mapped_df=fromnode_df, dataframecollection=dataframecollections, verbose=args.verbose)

    # At this point, dataframecollections have all the transformed data but is missing the foreign key fields from relationships
    if args.verbose >= 1:
        print("Creating to_mdf object")
    to_mdf = bento_mdf.MDF(*configs['lift_to_model_files'])

    #This adds the foreign keys, but with NaN as a value
    if args.verbose >= 1:
        print("Adding foreign keys")
    dataframecollections = addEdgeKeys(dataframecollections, to_mdf)
    # And time to go populate the foreign keys
    if args.verbose >= 1:
        print("Populating foreign keys")
    dataframecollections = populateEdgeKeys(dataframecollections, transform_df, conn)

    #if args.verbose >= 1:
    #    print("Building transformed relationships")
    #buildTransformedRelationships(to_mdf, list(dataframecollections.keys()), conn)
    '''for to_node, transformed_df in dataframecollections.items():
        edgelist = to_mdf.model.edges_by_src(to_mdf.model.nodes[to_node])
        edgenames = []
        for edge in edgelist:
            edgenames.append(edge.dst.handle)
        print(f"Node: {to_node}\tEdges: {edgenames}")'''


    #Write out the load sheets
    if args.verbose >= 1:
        print (f"Writing transformed load sheets to {configs['outputdir']}")
    writeTransformedLoadsheets(dataframecollections, configs['outputdir'])


    # The hard part - add target relationships after transformation or as part of mapping?
    # Can't do it like when loading from_data as the various fields may not be there
    # SOOOOOOO
    # Get all of the destination handles for a node
    # Check to see if the node exists in the transformed data in database (where label="to_nodename")
    # If so, query for all the to_nodenames and try to add the key field.



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument('-v', '--verbose', action='count', default=0, help=("Verbosity: -v main section -vv subroutine messages -vvv data returned shown"))

    args = parser.parse_args()

    main(args)