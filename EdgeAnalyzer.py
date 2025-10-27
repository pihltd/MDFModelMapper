import bento_mdf
from crdclib import crdclib
import argparse


def main(args):
    configs = crdclib.readYAML(args.configfile)

    mdf = bento_mdf.MDF(*configs['lift_to_model_files'])
    nodes = mdf.model.nodes
    


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument('-v', '--verbose', action='count', default=0, help=("Verbosity: -v main section -vv subroutine messages -vvv data returned shown"))

    args = parser.parse_args()

    main(args)

