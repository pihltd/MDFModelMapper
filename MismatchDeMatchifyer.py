import pandas as pd
import numpy as np
import argparse
from crdclib import crdclib



def main(args):
    configs = crdclib.readYAML(args.configfile)

    mapped_df = pd.read_csv(configs['filepath']+configs['mappingfile'], sep="\t")
    mismatch_df = pd.read_csv(configs['filepath']+configs['mismatchfile'], sep="\t")

   # Need deletes covering with CDE and without CDE

    # https://www.statology.org/pandas-drop-rows-based-on-multiple-conditions/
    # https://saturncloud.io/blog/how-to-delete-dataframe-rows-in-pandas-based-on-column-value/
    for index, row in mismatch_df.iterrows():
        mapped_df = mapped_df.drop(mapped_df[(
                (mapped_df['lift_from_node'] == row['lift_from_node']) &
                (mapped_df['lift_from_prop'] == row['lift_from_prop']) &
                (mapped_df['lift_from_model'] == row['lift_from_model']) &
                (mapped_df['lift_from_version'] == row['lift_from_version']) &
                (mapped_df['lift_to_node'] == row['lift_to_node']) &
                (mapped_df['lift_to_prop'] == row['lift_to_prop']) &
                (mapped_df['lift_to_model'] == row['lift_to_model']) &
                (mapped_df['lift_to_version'] == row['lift_to_version']))].index)

    mapped_df.to_csv(configs['filepath']+configs['savefile'], sep="\t", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument("-f", "--filename", help="Name of the file to analyze")
    parser.add_argument('-v', '--verbose', action='count', default=0, help=("Verbosity: -v main section -vv subroutine messages -vvv data returned shown"))

    args = parser.parse_args()

    main(args)