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


def fieldPopulator(fieldname, df, conn, verbose = 0):
    #if verbose >= 2:
    print(f"fieldPopulator checking for {fieldname}")
    # Works
    if fieldname == 'participant.study.study_id':
        df = populateStudy_Study_id(df, conn)
    # Works
    elif fieldname == 'sample.participant.study_participant_id':
        df=populateParticipant_study_participant_id(fieldname, df, conn)
    # File related
    # participant.study_participant_id Working (combineed with Sample)
    # study.study_id  WORKING
    # sample.sample_id  WORKING
    elif fieldname == 'sequencing_file.study.study_id':
        df = populateStudy_Study_id(df, conn)
    elif fieldname == 'sequencing_file.sample.sample_id':
        df = populateFileSampleSample_id(fieldname, df, conn)
    # Diagnosis doesn't have a sample reference but does have participant
    elif fieldname == 'diagnosis.participant.study_participant_id':
        df = populateDiagnosisStudyParticipant(fieldname, df, conn)
    elif fieldname == 'sequencing_file.file.file_id':
        df =  populateGenomicInfoFileFileID(df, conn)

    return df

def getStudyID(node, field, conn):
    query = cqb.cypherGetBasicNodeQuery(node)
    results = conn.query(query=query, db='neo4j')
    studyId = results[0][node][field]
    return studyId


def combofieldBreakUp(combofield):
    combolist = combofield.split(".")
    searchnode = combolist[0]
    linknode = combolist[1]
    field = combolist[2]
    return searchnode, linknode, field

def getParticipantID(node, field, value, conn):
    sample_query = cqb.cypherSingleWhereQuery(node, field, value)
    sample_results = conn.query(query=sample_query, db='neo4j')
    participant_id = sample_results[0][node.lower()]['participant.participant_id']
    return participant_id


def  populateGenomicInfoFileFileID(df, conn):
    for index, row in df.iterrows():
        query = cqb.cypherElementIDQuery(row['parent_elementId'])
        results = conn.query(query=query, db='neo4j')
        fileid = results[0]['s']['id']
        df.loc[index, 'file.file_id'] = fileid
    return df


def populateDiagnosisStudyParticipant(combofield, df, conn):
    searchnode, linknode, field = combofieldBreakUp(combofield)
    study_id = getStudyID('study', 'study_id', conn)
    for index, row in df.iterrows():
        query = cqb.cypherElementIDQuery(row['parent_elementId'])
        results = conn.query(query=query, db='neo4j')
        participant_id = results[0]['s'][f"{linknode}.{'participant_id'}"]
        df.loc[index, 'participant.study_participant_id'] = f"{study_id}_{participant_id}"
    return df


def populateFileSampleSample_id(combofield, df, conn):
    # This should be in the original record
    searchnode, linknode, field = combofieldBreakUp(combofield)
    study_id = getStudyID('study', 'study_id', conn)
    for index, row in df.iterrows():
        query = cqb.cypherElementIDQuery(row['parent_elementId'])
        results = conn.query(query=query, db='neo4j')
        id = results[0]['s'][f"{linknode}.{'sample_id'}"]
        df.loc[index, f"{linknode}.{field}"] = id
        # And the best time to get the participant ID is when we KNOW we've got a sample ID
        participant_id = getParticipantID(linknode, field, id, conn)
        df.loc[index, 'participant.study_participant_id'] = f"{study_id}_{participant_id}"
    return df


def populateStudy_Study_id(df, conn):
    node = 'study'
    field = 'study_id'
    # There should be only one row for study
    countquery = cqb.cypherRecordCount(node)
    countres = conn.query(query=countquery, db='neo4j')
    if  countres[0]['count'] == 1:
        studyId = getStudyID(node, field, conn)
        df[f"{node}.{field}"] = studyId
    return df

def populateParticipant_study_participant_id(combofield, df, conn):
    combolist = combofield.split(".")
    searchnode = combolist[0]
    linknode = combolist[1]
    field = combolist[2]
    studynode = 'study'
    studyfield = 'study_id'
    #print(f"Combofield: {combofield} Combolist: {combolist}\nSearchnode: {searchnode}\tLinknode: {linknode}\t Field: {field}")
    #searchnode = 'sample'
    #linknode= 'participant'
    #field = 'participant_id'
    studyid = getStudyID(studynode, studyfield, conn)
    for index, row in df.iterrows():
        elid = row['parent_elementId']
        query = cqb.cypherElementIDQuery(elid)
        #print(query)
        results = conn.query(query=query, db='neo4j')
        pid = results[0]['s'][f"{linknode}.{'participant_id'}"]
        spid = f"{studyid}_{pid}"
        df.loc[index, f"{linknode}.{field}"] = spid
    return df





'''def buildDestinationDataframes2(dataframecollection, fromnode_df, lift_to_mdf):
    to_nodes = fromnode_df['lift_to_node'].unique().tolist()
    for to_node in to_nodes:
        lift_to_df = fromnode_df.query('lift_to_node == @to_node')
        proplist = lift_to_df['lift_to_prop'].unique().tolist()
        proplist.append('parent_elementId')
        # Now have all mapped properties, need to get edge related properties
        # NOTE: The to_node should be the MDF relationship src node. The key fields from the dst node should be added to the src node load sheet. 
        if to_node in lift_to_mdf.model.nodes.keys():
            edgelist = lift_to_mdf.model.edges_by_src(lift_to_mdf.model.nodes[to_node])
            for edge in edgelist:
                dstnode = edge.dst.handle
                print(f"SrcNode is  {dstnode}")
                srckeylist = mdfTools.getKeyProperty(node=dstnode, mdf=lift_to_mdf)
                for srckey in srckeylist:
                    print(f"Adding {dstnode}.{srckey}")
                    proplist.insert(0, f"{dstnode}.{srckey}")

    return dataframecollection'''

def buildDestinationDataframes(dataframecollection, fromnode_df, lift_to_mdf):
    # Need a list of the destination nodes
    to_nodes = fromnode_df['lift_to_node'].unique().tolist()
    for to_node in to_nodes:
        lift_to_df = fromnode_df.query('lift_to_node == @to_node')
        proplist = lift_to_df['lift_to_prop'].unique().tolist()
        proplist.append('parent_elementId')
        # Now have all mapped properties, need to get edge related properties
        # NOTE: The to_node should be the MDF relationship src node. The key fields from the dst node should be added to the src node load sheet. 
        if to_node in lift_to_mdf.model.nodes.keys():
            #print(f"\nGetting edge list for {to_node}")
            edgelist = lift_to_mdf.model.edges_by_src(lift_to_mdf.model.nodes[to_node])
            for edge in edgelist:
                dstnode = edge.dst.handle
                #print(f"SrcNode is  {dstnode}")
                srckeylist = mdfTools.getKeyProperty(node=dstnode, mdf=lift_to_mdf)
                for srckey in srckeylist:
                    #print(f"Adding {dstnode}.{srckey}")
                    proplist.insert(0, f"{dstnode}.{srckey}")
            # Now, if the to_node is already in the collection, need to merge lists
            if to_node in dataframecollection.keys():
                existing_df = dataframecollection[to_node]
                existing_columns = existing_df.columns.tolist()
                proplist.extend(existing_columns)
                final = []
                for prop in proplist:
                    if prop not in final:
                        final.append(prop)
                new_df = pd.DataFrame(columns=final)
                dataframecollection[to_node] = new_df
            else:
                dataframecollection[to_node] = pd.DataFrame(columns=proplist)
    return dataframecollection



def buildEdgeKeys(lift_to_nodes, lift_to_mdf, verbose = 0):
    finallist = []
    if verbose >= 2:
        print(f"Building edge keys for node: {lift_to_nodes}")
    for ltn in lift_to_nodes:
        #print(f"Lift to node for getKeyProperty: {ltn}")
         # NOTE: The to_node should be the MDF relationship src node. The key fields from the dst node should be added to the src node load sheet. 
        if ltn in lift_to_mdf.model.nodes.keys():
            edgelist = lift_to_mdf.model.edges_by_src(lift_to_mdf.model.nodes[ltn])
            for edge in edgelist:
                dstnode = edge.dst.handle
                #print(f"SrcNode is  {dstnode}")
                srckeylist = mdfTools.getKeyProperty(node=dstnode, mdf=lift_to_mdf)
                for srckey in srckeylist:
                    #print(f"Adding {dstnode}.{srckey}")
                    finallist.insert(0, f"{dstnode}.{srckey}")

    return finallist



def buildTransformLoadsheets(movenode, movenode_df, conn, loadsheets, lift_to_mdf):
    # List of the mapped lift_from_props
    lift_from_props = movenode_df['lift_from_prop'].unique().tolist()
    lift_to_nodes = movenode_df['lift_to_node'].unique().tolist()
    #print(f"Lift to nodes: {lift_to_nodes}")
    # Need to get db results for the node
    movenodequery = cqb.cypherGetNodeQuery(movenode)
    movenoderesults = conn.query(query=movenodequery, db='neo4j')
    for movenoderesult in movenoderesults:
        loadline = {}
        #movenoderesult should be a neo4j Result object
        movenodedata = movenoderesult[movenode]
        #movenodedata shoudl be a neo4j Node object
        for prop in lift_from_props:
            loadline[prop] = movenodedata[prop]
        loadline['parent_elementId'] = movenoderesult['elid']
        #print(f"Pass 1: {loadline}")
        #keyproplist = buildNodeKeys(lift_to_nodes, lift_to_mdf)
        #print(f"For starting node {movenode} Starting db fields {list(movenodedata.keys())}")
        #print(f"Starting key property list {keyproplist}")
        #for keyprop in keyproplist:
            ##print(f"Starting keyprop is {keyprop}")
            #justprop = list(keyprop.split("."))[-1]
            #if keyprop in movenodedata.keys():
            #    #print(f"Found {keyprop} in movenodedata.keys")
            #    loadline[keyprop] = movenodedata[keyprop]
            #elif justprop in movenodedata.keys():
                #print(f"Found {justprop} in movenodedata keys")
            #    loadline[keyprop] =  movenodedata[justprop]
        #print(f"Pass 2: {loadline}")

        for lift_to_node in lift_to_nodes:
            temp_df = loadsheets[lift_to_node]
            temp_df.loc[len(temp_df)] = loadline
            loadsheets[lift_to_node] = temp_df
    return loadsheets


def addEdgeInfo(movenode, movenode_df, conn, loadsheets, lift_to_mdf):
    lift_from_props = movenode_df['lift_from_prop'].unique().tolist()
    lift_to_nodes = movenode_df['lift_to_node'].unique().tolist()
    for lift_to_node in lift_to_nodes:
        keyproplist = buildEdgeKeys([lift_to_node], lift_to_mdf)
        for keyprop in keyproplist:
            new_df = fieldPopulator(f"{movenode}.{keyprop}",loadsheets[lift_to_node],conn)
            loadsheets[lift_to_node] = new_df
    return loadsheets



def writeTransformedLoadsheets(dataframecollection, outputdir):

    for node, df in dataframecollection.items():
        filename = f"{outputdir}{node}_TRANSFORMED.csv"
        df.to_csv(filename, sep="\t", index=False)


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
        print("Building lift_to_ model")
    lift_to_mdf = bento_mdf.MDF(*configs['lift_to_model_files'])

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
        dataframecollections = buildDestinationDataframes(dataframecollections, fromnode_df, lift_to_mdf)

    #for node, df in dataframecollections.items():
    #    print(f"Node: {node}\nColumns: {df.columns.tolist()}\n")
    loadsheets = {}
    # For every node in the database, need to move the db information into a transformed load sheet

    #movenodelist = ['participant', 'sample']
    #movenodelist = list(dataframecollections.keys())
    #for movenode in movenodelist:
    for movenode in fromnodelist:
        movenode = movenode.lower()
        # Need to worry about case when using fromnodelist
        movenode_df = transform_df.query('lift_from_node == @movenode')
        loadsheets = buildTransformLoadsheets(movenode, movenode_df, conn, dataframecollections, lift_to_mdf)
        loadsheets = addEdgeInfo(movenode, movenode_df, conn, loadsheets, lift_to_mdf)

    #for node, loadsheet in loadsheets.items():
    #    print(f"Node: {node}\n{loadsheet.head(5)}")

    writeTransformedLoadsheets(loadsheets, configs['outputdir'])



        



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument('-v', '--verbose', action='count', default=0, help=("Verbosity: -v main section -vv subroutine messages -vvv data returned shown"))

    args = parser.parse_args()

    main(args)