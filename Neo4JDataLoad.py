import neo4j
import bento_mdf
import pandas as pd
import argparse
import os
from crdclib import crdclib


# https://medium.com/data-science/create-a-graph-database-in-neo4j-using-python-4172d40f89c4
# https://www.tutorialspoint.com/neo4j/neo4j_cql_creating_nodes.htm
# https://prvnk10.medium.com/mastering-the-create-clause-in-neo4j-a-beginners-guide-to-node-creation-7f9ecc6d0ee5

class Neo4jConnection:
    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None

        try:
            self.__driver = neo4j.GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
            print('Connection Succeeded')
        except Exception as e:
            print(f"Failed Connection:\n{e}")

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, parameters=None, db=None):
        assert self.__driver is not None, "Driver is not initialized"
        session = None
        response = None
        try:
            session = self.__driver.session(database=db) if db is not None else self.__driver.session()
            response = list(session.run(query, parameters))
        except Exception as e:
            print(f"Query failure:\n{e}")
        finally:
            if session is not None:
                session.close()
        return response



def getKeyProp(node, mdf):
    keys = []
    proplist = mdf.model.nodes[node].props
    for prop in proplist:
        if mdf.model.props[(node,prop)].get_attr_dict()['is_key'] == 'True':
            keys.append(prop)
    return keys



def getProps(filename):
    df = pd.read_csv(filename, sep=",")
    return df.columns.tolist()

def buildLoadCSVQuery(node, filename, mdf):
    returnstring = None
    startstring = f"LOAD CSV WITH HEADERS FROM 'file:///{os.path.basename(filename)}' AS row"
    # Need key property for node
    keyproplist = getKeyProp(node, mdf)
    if len(keyproplist) > 1:
        print(f"Key Property ID Error for Ndoe {node}\n{keyproplist}")
    else:
        keyprop = keyproplist[0]
        mergestring = f" MERGE ({node.lower()}:{node.upper()} {{{keyprop}:row.{keyprop}}})"
        returnstring = startstring+mergestring
        #Need a list of the remaining properties
        proplist = getProps(filename)
        proplist.remove(keyprop)
        oncreatestring = " ON CREATE SET"
        for prop in proplist:
            if "." in prop:
                prop = f"`{prop}`"
            if oncreatestring == ' ON CREATE SET':
                oncreatestring = oncreatestring+f" {node.lower()}.{prop} = row.{prop}"
            else:
                oncreatestring = oncreatestring+f", {node.lower()}.{prop} = row.{prop}"
        returnstring = returnstring+oncreatestring
    return returnstring


def buildRelationshipQuery(src, mdf, dst=None):
    edgequery = None
    if dst is not None:
        edgelist = [dst]
    else:
        edgelist = mdf.model.edges_by_src(mdf.model.nodes[src])
    for edge in edgelist:
            #need the key property from the dst node
            if dst is not None:
                keyproplist = getKeyProp(dst, mdf)
                print(f"DST key prop: {keyproplist}")
            else:
                keyproplist = getKeyProp(edge.dst.handle, mdf)
            if len(keyproplist) == 1:
                keyprop = keyproplist[0]
                if dst is None:
                    dst = edge.dst.handle
                edgequery = f"MATCH ({src.lower()}:{src.upper()}), ({dst.lower()}:{dst.upper()})"
                wherestring = f" WHERE {src.lower()}.`{dst.lower()}.{keyprop.lower()}` = {dst.lower()}.{keyprop.lower()}"
                edgequery = edgequery+wherestring
                createstring = f" CREATE ({src.lower()})-[:OF_{src.upper()}]->({dst.lower()})"
                edgequery = edgequery+createstring
    return edgequery


def main(args):

    if args.verbose >= 1:
        print(f"Reading configs from {args.configfile}")
    configs = crdclib.readYAML(args.configfile)

    if args.verbose >= 1:
        print("Creating MDF object")
    mdf = bento_mdf.MDF(*configs['mdffiles'])

    if args.verbose >= 1:
        print("Creating neo4j connection")
    conn = Neo4jConnection(os.getenv('NEO4J_URI'), os.getenv('NEO4J_USERNAME'), os.getenv('NEO4J_PASSWORD'))

    filelist = configs['sourcefiles']
    if configs['dataload']:
        if args.verbose >= 1:
            print("Starting data load")
        for entry in filelist:
            node = list(entry.keys())[0]
            filename = list(entry.values())[0]
            query = buildLoadCSVQuery(node, filename, mdf)
            if args.verbose >= 2:
                print(query)
            # Load data if there's an actual query
                if query is not None:
                    conn.query(query=query, db='neo4j')

    # Yes, this looks weird, but the data has to be loaded first before edges can be added
    if configs['edges']:
        if args.verbose >= 1:
            print("Starting relationship addition")
        for entry in filelist:
            node = list(entry.keys())[0]
            edgequery = buildRelationshipQuery(src=node, mdf=mdf)
            if edgequery is not None:
                if args.verbose >=2:
                    print(edgequery)
                conn.query(query=edgequery, db='neo4j')
        manualedges = configs['manualedges']
        for manualedge in manualedges:
            edgequery = buildRelationshipQuery(src=list(manualedge.keys())[0], dst=list(manualedge.values())[0], mdf=mdf)
            if edgequery is not None:
                if args.verbose >= 2:
                    print(edgequery)
                conn.query(query=edgequery, db='neo4j')
            else:
                print("Edgequery is none")
        
        


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument('-v', '--verbose', action='count', default=0, help=("Verbosity: -v main section -vv subroutine messages -vvv data returned shown"))

    args = parser.parse_args()

    main(args)