import argparse
from crdclib import crdclib
import pandas as pd

def buildSourceSheets(xlfilepath):

    sourcesheets = {}

    xlfile = pd.ExcelFile(xlfilepath)
    for node in xlfile.sheet_names:
        if node not in ['Dictionary', 'Terms and Value Sets', 'README and INSTRUCTIONS']:
            temp_df = pd.read_excel(xlfilepath, sheet_name=node, engine='openpyxl')
            if 'type' in temp_df.columns.tolist():
                # Clear out the type column, we'll add it back later
                temp_df = temp_df.drop('type', axis=1)
                #Drop all rows that only have NaN
                temp_df = temp_df.dropna(axis=0, how='all')
                # Drop all columns that are only NaN
                temp_df = temp_df.dropna(axis=1, how='all')
            #Only load up sheets that aren't empty
            if not temp_df.empty:
                sourcesheets[node] = temp_df
    return sourcesheets


def main(args):

    ccdi_excel = '/media/vmshare/CCDI/phs003519_CCDI_Study_Manifest 1.xlsx'
    outdir = '/media/vmshare/CCDI/csv/'
    sourcesheets = buildSourceSheets(ccdi_excel)

    for node, sourcesheet in sourcesheets.items():
        filename = f"{outdir}{node}_CCDI.csv"
        sourcesheet.to_csv(filename, sep=",", index=False )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    #parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument('-v', '--verbose', action='count', default=0, help=("Verbosity: -v main section -vv subroutine messages -vvv data returned shown"))

    args = parser.parse_args()

    main(args)