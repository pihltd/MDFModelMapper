import bento_mdf
import pandas as pd
import numpy as np
import argparse
from crdclib import crdclib
import math
# TODO:
# Number of mapped properties compared to total
# CDE Reuse: Number of times as CDE is used in mapping
# Node/Property mismatch:
#  Same CDE, Different properties
# Same properties, different nodes

def mdfToDF(mdf, verbose=False):
    # Returns a dataframe of nodes and their properties
    columns = ['handle', 'version','node', 'property']
    data_df = pd.DataFrame(columns=columns)
    props = mdf.model.props
    for prop in props:
        if verbose:
            print(prop)
        data_df.loc[len(data_df)] = {'handle': mdf.handle, 'version': mdf.version,'node':prop[0], 'property':prop[1]}
    return data_df
    


def basicCounts(from_df, to_df, mapping_df, verbose=False):
    from_mapped_list = mapping_df['lift_from_prop'].unique()
    to_mapped_list = mapping_df['lift_to_prop'].unique()
    
    #print(f"to_df Value Counts:\n{to_df.value_counts()}")
    print(f"Total properties for from_df:\t{len(from_df)}")
    print(f"Total mapped properties for from_df:\t{len(from_mapped_list)}")
    print(f"Total properties for to_df:\t{len(to_df)}")
    print(f"Total mapped propertis for to_df:\t{len(to_mapped_list)}")



def unmappedPropsDF(df, mapping_df, propfield, verbose=False):
    columns = ['handle', 'version','node', 'property']
    unmapped_df = pd.DataFrame(columns=columns)
    mappedList = mapping_df[propfield].unique()
    for index, row in df.iterrows():
        if row['property'] not in mappedList:
            unmapped_df.loc[len(unmapped_df)] = row
    return unmapped_df

def addRow(df, row, errorstring):
    #print(row)
    df.loc[len(df)] = {
                'lift_from_node': row['lift_from_node'],
                'lift_from_prop': row['lift_from_prop'],
                'lift_from_cdeID': row['lift_from_cdeID'],
                'lift_from_cdeVersion': row['lift_from_cdeVersion'],
                'lift_from_model': row['lift_from_model'],
                'lift_from_version': row['lift_from_version'],
                'lift_to_node': row['lift_to_node'],
                'lift_to_prop': row['lift_to_prop'],
                'lift_to_cdeID': row['lift_to_cdeID'],
                'lift_to_cdeVersion': row['lift_to_cdeVersion'],
                'lift_to_model': row['lift_to_model'],
                'lift_to_version': row['lift_to_version'],
                'mapping_type': errorstring
            }
    return df
    
def mismatchCheck(mapfile, verbose):
    columns = ['lift_from_node', 'lift_from_prop', 'lift_from_cdeID', 'lift_from_cdeVersion', 'lift_from_model', 'lift_from_version', 'lift_to_node', 'lift_to_prop', 'lift_to_cdeID', 'lift_to_cdeVersion', 'lift_to_model' ,'lift_to_version', 'mapping_type']
    mismatch_df = pd.DataFrame(columns=columns)
    if verbose >= 2:
        print(f"Reading {mapfile}")
    mapped_df = pd.read_csv(mapfile, sep="\t")
    for index, row in mapped_df.iterrows():
        if verbose >=3:
            print(row)
        if row['lift_from_prop'] != row['lift_to_prop']:
            mismatch_df= addRow(mismatch_df, row, 'Property Name Mismatch')
        elif row['lift_from_node'] != row['lift_to_node']:
            mismatch_df = addRow(mismatch_df, row, 'Node Mismatch')
        elif row['lift_from_cdeID'] != row['lift_to_cdeID']:
            if verbose >=3:
                print(f"Isnan check on {row['lift_from_cdeID']}")
            if row['lift_from_cdeID'] is not np.nan:
            #if not np.isnan(row['lift_from_cdeID']):
                mismatch_df = addRow(mismatch_df, row, 'CDE ID mismatch')
    return mismatch_df
            
    
    
def main(args):
    configs = crdclib.readYAML(args.configfile)
    lift_from_model = bento_mdf.MDF(*configs['lift_from_model_files'])
    lift_to_model = bento_mdf.MDF(*configs['lift_to_model_files'])
    
    if args.filename is None:
    # If no mapping filename is provided, we can guess from the standard way MDFModelMapper creates files
        propmapfilename = f"{lift_from_model.model.handle}_{lift_from_model.model.version}-{lift_to_model.model.handle}_{lift_to_model.model.version}.tsv"
    else:
        propmapfilename = args.filename
    # Autoname report file for now.  
    reportfile = f"{lift_from_model.model.handle}_{lift_from_model.model.version}-{lift_to_model.model.handle}_{lift_to_model.model.version}_MISMATCH_REPORT.tsv"
    
    #lift_from_df = mdfToDF(lift_from_model)
    #print(lift_from_df)
    #lift_to_df = mdfToDF(lift_to_model, True)
    #print(lift_to_df['node'].unique())
    #if args.filename is not None:
    
    if args.verbose >= 1:
        print("Starting mismatch check")
    mismatch_df = mismatchCheck(configs['savepath']+propmapfilename, args.verbose)
    if args.verbose >= 1:
        print("Saving mismatch file")
    mismatch_df.to_csv(configs['savepath']+reportfile, sep="\t", index=False)
    
    '''
    mapping_df = pd.read_csv(configs['mapping_file'], sep="\t")
    
    basicCounts(lift_from_df, lift_to_df, mapping_df)
    
    lift_from_unmapped_df = unmappedPropsDF(lift_from_df, mapping_df, 'lift_from_prop')
    #print(lift_from_unmapped_df)
    lift_to_unmapped_df = unmappedPropsDF(lift_to_df, mapping_df, 'lift_to_prop')
    print(f"Unmapped From {lift_from_model.handle}\n{lift_from_unmapped_df['node'].value_counts()}")
    print(f"Unmapped From {lift_to_model.handle}\n{lift_to_unmapped_df['node'].value_counts()}")
'''


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument("-f", "--filename", help="Name of the file to analyze")
    #parser.add_argument("-v", "--verbose", action='store_true', help="Verbose Output")
    parser.add_argument('-v', '--verbose', action='count', default=0, help=("Verbosity: -v main section -vv subroutine messages -vvv data returned shown"))

    args = parser.parse_args()

    main(args)