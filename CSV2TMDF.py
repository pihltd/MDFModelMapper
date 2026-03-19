# Reads a CCDI Liftover file and tries to create a Transform MDF out of the thing.
import argparse
import pandas as pd
from crdclib import crdclib
import sys


def uniqueValue(df, column):
    unique_list = df[column].unique().tolist()
    if len(unique_list) == 1:
        return unique_list[0]
    else:
        return None

def modelInfo(mapping_json, mapping_df, verbose = 0):
    lift_from_model_handle = uniqueValue[mapping_df, 'lift_from_model']
    lift_from_model_version = uniqueValue[mapping_df, 'lift_from_version']
    lift_to_model_name = uniqueValue[mapping_df, 'lift_to_model']
    lift_to_model_version = uniqueValue[mapping_df, 'lift_to_version']
    



def main (args):

    if args.verbose > 1:
        print(f"Reading configuration file {args.confgifile}")
    configs = crdclib.readYAML(args.configfile)

    mappingfile = f"{configs['savepath']}{configs['savefile']}"
    if args.verbose > 1:
        print(f"Reading mapping file {mappingfile}")
    mapping_df = pd.read_csv(mappingfile, sep="\t")

    # Set up the basics for Input and Output

    mapping_json = {}

    lift_from_model_handle = uniqueValue(mapping_df, 'lift_from_model')
    lift_from_model_version = uniqueValue(mapping_df, 'lift_from_version')
    lift_to_model_handle = uniqueValue(mapping_df, 'lift_to_model')
    lift_to_model_version = uniqueValue(mapping_df, 'lift_to_version')

    mapping_json['TransformDefinitions'] = {'Defaults': {'Inputs': {'Model': lift_from_model_handle, 'Version': lift_from_model_version}}}

    mapping_json['TransformDefinitions']['Defaults']['Outputs'] = {'Model': lift_to_model_handle, 'Version': lift_to_model_version}

    if args.verbose >= 1:
        print(mapping_json)

    # Add identities
    identityList = []
    transformList = []
    # If a lift_from_property maps to 1 and only 1
    # Need a nlist of unique from nodes
    fromNodes = mapping_df['lift_from_node'].unique().tolist()
    
    for fromNode in fromNodes:
        node_df = mapping_df.query('lift_from_node == @fromNode')
        fromProps = node_df['lift_from_prop'].unique().tolist()
        for fromProp in fromProps:
            prop_df = node_df.query('lift_from_prop == @fromProp')
            if len(prop_df) == 1:
                identityList.append({'From': {'Node': fromNode, 'Prop': fromProp}, 'To': {'Node':prop_df.iloc[0]['lift_to_node'], 'Prop':prop_df.iloc[0]['lift_to_prop']}})
            else:
                #Multiple Inputs to 1 output?
                #Single INput to multiple outputs?
                # MAny inputs to many outputs?  YIKES!  Not going to do that juuust yet.
                #Start with getting unique counts
                lfnc = prop_df['lift_from_node'].nunique()
                lfpc = prop_df['lift_from_prop'].nunique()
                ltnc = prop_df['lift_to_node'].nunique()
                ltpc = prop_df['lift_to_prop'].nunique()
                print(f"From Node: {fromNode}\tFromProp: {fromProp}\nlfnc: {lfnc}\tlfpc: {lfpc}\tltnc: {ltnc}\tltpc: {ltpc}\n")
                '''for index, row in prop_df.iterrows():
                    tname = f"{row['lift_from_node']}_{row['lift_from_prop']}_2_{row['lift_to_node']}_{row['lift_to_prop']}"
                    transformList.append(
                        tname:{
                            'Inputs':[{'Model': lift_from_model_handle, 'Version': lift_from_model_version}, 'Node':row['lift_from_node'], 'Prop': row['lift_from_prop']],
                            'Outputs'
                        }
                    )'''
    
    mapping_json['Identities'] = identityList

    crdclib.writeYAML(f"{configs['savepath']}{configs['tmdffile']}",mapping_json)
                
        #print(f"For Node {fromNode}\n{node_df['lift_from_prop'].value_counts()}\n")

    #print(mapping_df['lift_from_prop'].value_counts())






if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument('-v', '--verbose', action='count', default=0, help=("Verbosity: -v main section -vv subroutine messages -vvv data returned shown"))

    args = parser.parse_args()

    main(args)