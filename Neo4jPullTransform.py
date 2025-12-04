# Rather than push from CCDI to GC, can we pull to GC from CCDI.  In other words, we establishe the GC loading sheets and then pull based on the mappingimport warnings
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


        
def addElementID(loadsheets_df):
    for node, loadsheet in loadsheets_df.items():
        columns = loadsheet.columns.tolist()
        columns.append('parent_elementId')
        temp_df = pd.DataFrame(columns=columns)
        loadsheets_df[node] = temp_df
    return loadsheets_df


def nodeTrimmer(nodelist, mapping_df):
    returnlist = []
    for node in nodelist:
        node = node.lower()
        #print(f"Node: {node}")
        temp_df = mapping_df.query('lift_from_node == @node')
        templist = temp_df['lift_to_node'].unique().tolist()
        returnlist= list(set(returnlist + templist))
    return returnlist


def writeTransformedLoadsheets(dataframecollection, outputdir):

    for node, df in dataframecollection.items():
        filename = f"{outputdir}GC_{node}_TRANSFORMED.csv"
        df.to_csv(filename, sep="\t", index=False)


def transformDecider(to_node, to_df, to_mapping_df, conn, dbnodelist):
    if to_node == 'study':
        print("Study")
        to_df = gc.gcStudyNode(to_df, to_mapping_df, conn, dbnodelist)
    elif to_node == 'sample':
        print("Sample")
        to_df == gc.gcSampleNode(to_df, to_mapping_df, conn, dbnodelist)
    elif to_node == 'participant':
        print("Participant")
        to_df = gc.gcParticipantNode(to_df, to_mapping_df, conn, dbnodelist)
    elif to_node == 'diagnosis':
        print("Diagnosis")
        to_df = gc.gcDiagnosisNode(to_df, to_mapping_df, conn, dbnodelist)
    elif to_node == 'genomic_info':
        print("Genomic Info")
        to_df = gc.gcGenomicInfoNode(to_df, to_mapping_df, conn, dbnodelist)
    elif to_node == 'file':
        print("File")
        to_df = gc.gcFileNode(to_df, to_mapping_df, conn, dbnodelist)
    return to_df


def nullRowRemover(loadsheets):
    returninfo = {}
    for node, loadsheet in loadsheets.items():
        loadsheet.dropna(axis=0, how='all', inplace=True)
        returninfo[node] = loadsheet
    return returninfo



def addModelName(loadsheets, modelhandle, modelversion):
    returninfo = {}
    for node, loadsheet in loadsheets.items():
        loadsheet['modelhandle'] = modelhandle
        loadsheet['modelversion'] = modelversion
        returninfo[node] = loadsheet
    return returninfo


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

    tonodelist = []
    tonodelist = nodeTrimmer(fromnodelist, transform_df)

    if args.verbose >= 1:
        print(f"Labels found in database: {fromnodelist}")

    # Get the empty load sheets for the to_model
    temp_to_loadsheets = crdclib.mdfBuildLoadSheets(lift_to_mdf)

    #NOTE: Hardcoded correction for the file_id property in file.
    file_df = temp_to_loadsheets['file']
    fileheaders = file_df.columns.tolist()
    if 'file_id' not in fileheaders:
        fileheaders.insert(0,'file_id')
    newdf = pd.DataFrame(columns=fileheaders)
    temp_to_loadsheets['file'] = newdf
   
    

    # Drop any nodes that aren't in the database
    to_loadsheets = {}
    for key, value in temp_to_loadsheets.items():
        if key in tonodelist:
            to_loadsheets[key] = value

     # Add a column for the parent elementId
    to_loadsheets = addElementID(to_loadsheets)

    #Clean up transform_df to drop all node lables not in the database
    for to_node in to_node_list:
        if to_node not in tonodelist:
            transform_df = transform_df[transform_df.lift_from_node != to_node]

    for to_node, to_df in to_loadsheets.items():
        to_mapping_df = transform_df.query('lift_to_node == @to_node')
        to_df = transformDecider(to_node, to_df, to_mapping_df, conn, fromnodelist)
        to_loadsheets[to_node] = to_df

    to_loadsheets = nullRowRemover(to_loadsheets)
    to_loadsheets = addModelName(to_loadsheets, lift_to_mdf.handle, lift_to_mdf.version)

    
    writeTransformedLoadsheets(to_loadsheets, configs['outputdir'])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument('-v', '--verbose', action='count', default=0, help=("Verbosity: -v main section -vv subroutine messages -vvv data returned shown"))

    args = parser.parse_args()

    main(args)