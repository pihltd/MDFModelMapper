import neo4j
import bento_mdf
import pandas as pd
import argparse
import os
from crdclib import crdclib
import sys
import logging
import uuid
sys.path.insert(1,'../CRDCTransformationLibrary/src')
import mdfTools
import Neo4JConnection as njc


'''class Neo4jConnection:
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
        return response'''



def  buildNodeQuery(fromnode, verbose = 0):
    # Queries the database for all fromnodes including all properties and element ID
    return f"MATCH ({fromnode.lower()}:{fromnode.upper()}) WITH *, elementID({fromnode.lower()}) AS elid RETURN {fromnode.lower()},elid"


def buildPropList(property, value, node_df, proplistmap, verbose=0):
    # Check to see if the property has been mapped
    if property in node_df['lift_from_prop'].unique().tolist():
        # Get the dataframe rows that match
        prop_df = node_df.query('lift_from_prop == @property')
        for index, row in prop_df.iterrows():
            to_node = f"lift_to_{row['lift_to_node']}"
            to_prop = row['lift_to_prop']
            if to_node in proplistmap.keys():
                templist = proplistmap[to_node]
                templist.append({to_prop:value})
                proplistmap[to_node] = templist
            else:
                proplistmap[to_node] = [{to_prop:value}]
    return proplistmap



def addElementId(newmapping, elementid, verbose=0):
    for node, properties in newmapping.items():
        if 'element_id' in properties:
            templist = properties['element_id']
            if elementid not in templist:
                templist.append(elementid)
                properties['element_id'] = templist
        else:
            properties['element_id'] = [elementid]
    newmapping[node] = properties
    return newmapping




def updateNewMapping(newmapping, proplistmap, verbose=0):
    # Adds the list of properties to the nodes
    for node, props in proplistmap.items():
        if node in newmapping.keys():
            temp = newmapping[node]
            temp.append(props)
            newmapping[node] = temp
        else:
            newmapping[node] = [props]
    return newmapping



def addOfTransformRelationships(src, conn, verbose=0):
    dst = "lift_to_"+src
    keypropsrc = 'parent_elementId'
    edgequery = f"MATCH ({src.lower()}:{src.upper()}), ({dst.lower()}:{dst.upper()})"
    wherestring = f" WHERE elementid({src.lower()}) = {dst.lower()}.{keypropsrc}"
    edgequery = edgequery+wherestring
    createstring = f" CREATE ({src.lower()})-[:OF_TRANSFORMATION]->({dst.lower()})"
    edgequery = edgequery+createstring
    if verbose >= 2:
        print(f"Relationship query:\n{edgequery}")
    conn.query(query=edgequery, db='neo4j')



def manualAddTransformRelationships(src, dst, conn, verbose=0):
    keypropsrc = 'parent_elementId'
    edgequery = f"MATCH ({src.lower()}:{src.upper()}), ({dst.lower()}:{dst.upper()})"
    wherestring = f" WHERE elementid({src.lower()}) = {dst.lower()}.{keypropsrc}"
    edgequery = edgequery+wherestring
    createstring = f" CREATE ({src.lower()})-[:OF_TRANSFORMATION]->({dst.lower()})"
    edgequery = edgequery+createstring
    if verbose >= 2:
        print(f"Relationship query:\n{edgequery}")
    conn.query(query=edgequery, db='neo4j')



def printTransformedFiles(newmapping, outputdir, verbose=0):
    nodefilelist = {}
    for nodename, nodelist in newmapping.items():
        nodeproplist = []
        for singlenodelist in nodelist:
            temp = {}
            for node in singlenodelist:
                for property, value in node.items():
                    temp[property] = value
            nodeproplist.append(temp)
        # Now get the columns
        columns = list(nodeproplist[0].keys())
        temp_df = pd.DataFrame(columns=columns)
        for entry in nodeproplist:
            temp_df.loc[len(temp_df)] = entry
        #print the file
        filename = outputdir+f"{nodename}_TRANSFORMED.csv"
        temp_df.to_csv(filename, sep="\t", index=False)
        nodefilelist[nodename] = filename
    return nodefilelist 



def buildLoadCSVQuery(node, filename, verbose=0):
    returnstring = None
    startstring = f"LOAD CSV WITH HEADERS FROM 'file:///{os.path.basename(filename)}' AS row FIELDTERMINATOR '\t'"
    # Need key property for node
    keyprop = 'parent_elementId'
    mergestring = f" MERGE ({node.lower()}:{node.upper()} {{{keyprop}:row.{keyprop}}})"
    returnstring = startstring+mergestring
        #Need a list of the remaining properties
    proplist = pd.read_csv(filename, sep="\t").columns.tolist()
    if verbose >= 2:
        print(proplist)
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



def getKeyProp(node, mdf, verbose=0):
    keys = []
    proplist = mdf.model.nodes[node].props
    for prop in proplist:
        if mdf.model.props[(node,prop)].get_attr_dict()['is_key'] == 'True':
            keys.append(prop)
    return keys



def buildRelationshipQuery(src, mdf, dst=None, verbose=0):
    edgequery = None
    if dst is not None:
        edgelist = [dst]
    else:
        edgelist = mdf.model.edges_by_src(mdf.model.nodes[src])
    for edge in edgelist:
            #need the key property from the dst node
            if dst is not None:
                keyproplist = getKeyProp(dst, mdf)
            else:
                keyproplist = getKeyProp(edge.dst.handle, mdf)
            if len(keyproplist) == 1:
                keyprop = keyproplist[0]
                if dst is None:
                    dst = edge.dst.handle
                newsrc = "LIFT_TO_"+ src
                newdst = "LIFT_TO_"+ dst
                edgequery = f"MATCH ({newsrc.lower()}:{newsrc.upper()}), ({newdst.lower()}:{newdst.upper()})"
                wherestring = f" WHERE {newsrc.lower()}.`{dst.lower()}.{keyprop.lower()}` = {newdst.lower()}.{keyprop.lower()}"
                edgequery = edgequery+wherestring
                createstring = f" CREATE ({newsrc.lower()})-[:OF_{newsrc.upper()}]->({newdst.lower()})"
                edgequery = edgequery+createstring
    return edgequery




def addCompoundKeys(compoundKeyList, conn, verbose=0):
    delimiter = "_"
    
    for entry in compoundKeyList:
        for targetnode, propstuff in entry.items():
            print(f"Target Node: {targetnode}\nPropstuff: {propstuff}")
            tnquery = f"MATCH (t:LIFT_TO_{targetnode.upper()}) RETURN t"
            tnresult = conn.query(query=tnquery, db='neo4j')
            for result in tnresult:
                pid = result.data()['t']['parent_elementId']
                # This query will return the original node the transform was pulled from
                # NEED a safety valve here, not every target node will have an OF_TRANSFORMATION relationship
                nodelist = []
                testquery = "MATCH (n) RETURN distinct labels(n)"
                testres = conn.query(query=testquery, db='neo4j')
                for entry in testres:
                    #print(entry.data()['labels(n)'][0])
                    nodelist.append(entry.data()['labels(n)'][0])
                
                if targetnode.upper() in nodelist:
                    relquery = f"MATCH (l:LIFT_TO_{targetnode.upper()})-[:OF_TRANSFORMATION]-(p:{targetnode.upper()}) WHERE l.parent_elementId = '{pid}' RETURN p"
                    relres = conn.query(query=relquery, db='neo4j')
                    for targetprop, sourcelist in propstuff.items():
                        keystring = None
                        if sourcelist == 'UUID':
                            keystring = uuid.uuid4()
                        else:
                            for each in sourcelist:
                                for sourcenode, sourceprop in each.items():
                                    if sourceprop in relres[0].data()['p']:
                                        if keystring is None:
                                            keystring = relres[0].data()['p'][sourceprop]
                                        else:
                                            keystring = f"{keystring}{delimiter}{relres[0].data()['p'][sourceprop]}"
                        setquery = f"MATCH (s:LIFT_TO_{targetnode.upper()}) WHERE s.parent_elementId = '{pid}' SET s.{targetprop} = '{keystring}'"
                        conn.query(query=setquery, db='neo4j')




def main(args):
    #print(f"Args.verbose is {str(args.verbose)}")
    if args.verbose >= 1:
        print("Parsing configuration file")
    configs = crdclib.readYAML(args.configfile)

    if args.verbose >= 1:
        print("Reading transformation files")
    transform_df = pd.read_csv(configs['transform_file'], sep="\t")

    fromnodelist = transform_df['lift_from_node'].unique().tolist()
    if args.verbose >= 1:
        print(f"FromNodeList: {fromnodelist}")

    if args.verbose >= 1:
        print("Setting SEVERE logging level")
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.ERROR)
        logging.getLogger('neo4j').addHandler(handler)
        logging.getLogger('neo4j').setLevel(logging.ERROR)


    if args.verbose >= 1:
        print("Establishing database connection")
    #conn = Neo4jConnection(os.getenv('NEO4J_URI'), os.getenv('NEO4J_USERNAME'),os.getenv('NEO4J_PASSWORD'))
    conn = njc.Neo4jConnection(os.getenv('NEO4J_URI'), os.getenv('NEO4J_USERNAME'),os.getenv('NEO4J_PASSWORD'))
    sys.exit(0)

    #fromnodelist = ['participant']
    #fromnodelist = ['study']

    newmapping = {}

    for fromnode in fromnodelist:
        if args.verbose >= 1:
            print(f"Starting work on FromNode: {fromnode}")
        fromnodequery = buildNodeQuery(fromnode, args.verbose)
        if args.verbose >= 2:
            print(f"Query for {fromnode}\n{fromnodequery}")
        node_df = transform_df.query('lift_from_node == @fromnode')
        #
        #print(node_df)
        #sys.exit(0)
        if args.verbose >= 2:
            print(f"Running {fromnodequery}")
        # Get all the existing nodes for the from node
        queryres = conn.query(query=fromnodequery, db='neo4j')
        if args.verbose >= 3:
            print(f"Query results for {fromnode}\n{queryres}")


        # For each entry in the returned result
        # for each property that is mapped
        # Get mapping info (to_node, to_prop)
        # Add to_prop:value to list for the to_node
        # Add elementId to list
        # Add list to to_node dictionary
        # newmapping:  Dictionary of list of lists of dictionary.  {Nodename:[[{entry1]}, [entry2], [entry3]]}


        # Each entry in the newmapping list are the mapped properties that can be used to create nodes
        for entry in queryres:
            proplistmap = {}
            resdata = entry.data()[fromnode.lower()] # This has all the properties for each node
            elementid = entry.data()['elid']
            for property, value in resdata.items():
                proplistmap = buildPropList(property, value, node_df, proplistmap, args.verbose)
                # At this point, proplistmap should have data from a single node
                #Add the elementID
            for node, proplist in proplistmap.items():
                proplist.append({'parent_elementId':elementid})
                proplistmap[node] = proplist
            # Add the properties to the nodes
            newmapping = updateNewMapping(newmapping, proplistmap, args.verbose)

        # And the extra fun part, add properties from the 'compound_keys' part of configs
        # And this is in the wrong place. As-is, the lift_to nodes haven't been created yet.
        # NEED TO MOVE
        addCompoundKeys(configs['compound_keys'], conn, args.verbose)

    # The nodes, properies, and values are all set in newmapping, print them to files if requested in configs
    if configs['makefiles']:
        if args.verbose >= 1:
            print(f"Printing transformed load sheets to {configs['outputdir']}")
        mappedfiledict = printTransformedFiles(newmapping, configs['outputdir'], args.verbose)


    # If requested in configs, load the files to the database
    if configs['loadfiles']:
        for node, filename in mappedfiledict.items():
            if args.verbose >= 1:
                print(f"Loading transformed data for node {node} from file {filename}")
            query = buildLoadCSVQuery(node, filename, args.verbose)
            if args.verbose >= 2:
                print(f"Loading Query:\n{query}")
            conn.query(query=query, db='neo4j')


    if configs['makeedges']:
        if args.verbose >= 1:
            print("Starting to add transformation relationships relationships")
        for fromnode in fromnodelist:
            if args.verbose >= 2:
                print(f"Setting relationships for node {fromnode}")
            addOfTransformRelationships(fromnode, conn, args.verbose)
        # Add manual
        manualAddTransformRelationships('sequencing_file', 'lift_to_file', conn, args.verbose)


    # There's a problem here:  If the key fields aren't mapped, there's no way to construct the relationships for the lift_to nodes.
    if configs['modeledges']:
        if args.verbose >= 1:
            print("Adding relationships for target model")
        mdf = bento_mdf.MDF(*configs['lift_to_model_files'])
        tonodelist = list(mdf.model.nodes)
        for tonode in tonodelist:
            query = buildRelationshipQuery(tonode, mdf)
            if args.verbose >= 2:
                print(f"Model relationship query:\n{query}")
            conn.query(query=query, db='neo4j')

    #At this point 2 sets of relationships to build:
    # 1) OF_TRANSFORM that links to the original node based on elementID  That can be done once the new node is added.
    # 2) The relationships from the new model.  This has to be done after all the new nodes are added.







if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument('-v', '--verbose', action='count', default=0, help=("Verbosity: -v main section -vv subroutine messages -vvv data returned shown"))

    args = parser.parse_args()

    main(args)