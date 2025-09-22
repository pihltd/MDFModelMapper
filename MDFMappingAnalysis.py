import bento_mdf
import pandas as pd
import argparse
from crdclib import crdclib
# TODO:
# Number of mapped properties compared to total
# CDE Reuse: Number of times as CDE is used in mapping

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
    
    
    
def main(args):
    configs = crdclib.readYAML(args.configfile)
    lift_from_model = bento_mdf.MDF(*configs['lift_from_model_files'])
    lift_to_model = bento_mdf.MDF(*configs['lift_to_model_files'])
    
    lift_from_df = mdfToDF(lift_from_model)
    #print(lift_from_df)
    lift_to_df = mdfToDF(lift_to_model, True)
    print(lift_to_df['node'].unique())
    
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
    parser.add_argument("-v", "--verbose", action='store_true', help="Verbose Output")

    args = parser.parse_args()

    main(args)