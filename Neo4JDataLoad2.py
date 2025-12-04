import neo4j
import bento_mdf
import pandas as pd
import argparse
import os
import sys
from crdclib import crdclib
sys.path.insert(1,'../CRDCTransformationLibrary/src')
import mdfTools
import Neo4JConnection as njc
import cypherQueryBuilders as cqb



def main(args):
    if args.verbose >= 1:
        print(f"Reading configs from {args.configfile}")
    configs = crdclib.readYAML(args.configfile)

    if args.verbose >= 1:
        print("Creating MDF object")
    mdf = bento_mdf.MDF(*configs['mdffiles'])

    if args.verbose >= 1:
        print("Creating neo4j connection")
    conn = njc.Neo4jConnection(os.getenv('NEO4J_URI'), os.getenv('NEO4J_USERNAME'), os.getenv('NEO4J_PASSWORD'))

    # Get the list of csv files to load into neo4j
    filelist = configs['sourcefiles']
    if configs['dataload']:
        if args.verbose >= 1:
            print("Starting data load")
        for entry in filelist:
            for node, filename in entry.items():
                if args.verbose >=2:
                    print(f"Creating load query for node {node} and file {filename}")
                if 'separator' in configs:
                    if configs['separator'] == 'comma':
                        propertylist = pd.read_csv(filename, sep=",").columns.to_list()
                    elif configs['separator'] == 'tab':
                        propertylist = pd.read_csv(filename, sep="\t").columns.to_list()
                else:
                    # Assume tab as default separator
                    propertylist = pd.read_csv(filename, sep="\t").columns.to_list()
                keyprop = mdfTools.getKeyProperty(node, mdf)[0]
                if args.verbose >=2:
                    print(f"Key property: {keyprop}\nProperty list: {propertylist}\n")
                query = cqb.cypherLoadCSVQuery(nodelabel=node, filename=filename, proplist=propertylist, keyprop=keyprop, separator=configs['separator'])
                if args.verbose >= 2:
                    print(query)
                # Load data if there's an actual query
                if query is not None:
                    conn.query(query=query, db='neo4j')



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument('-v', '--verbose', action='count', default=0, help=("Verbosity: -v main section -vv subroutine messages -vvv data returned shown"))

    args = parser.parse_args()

    main(args)