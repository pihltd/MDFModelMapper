# Loads a transformation CSV into neo4j
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
    configs = crdclib.readYAML(args.configfile)
    transform_df = pd.read_csv(configs['transform_file'], sep="\t")
    transform_columns = transform_df.columns.tolist()

    conn = njc.Neo4jConnection(os.getenv('NEO4J_URI'), os.getenv('NEO4J_USERNAME'), os.getenv('NEO4J_PASSWORD'))

    loadquery = cqb.cypherLoadCSVQuery('tranforminfo', configs['transform_file'], separator='tab', proplist=transform_columns, keyprop='lift_from_node')
    if args.verbose >= 1:
        print(loadquery)
    conn.query(query=loadquery)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument('-v', '--verbose', action='count', default=0, help=("Verbosity: -v main section -vv subroutine messages -vvv data returned shown"))

    args = parser.parse_args()

    main(args)