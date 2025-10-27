from crdclib import crdclib
import argparse
import pandas as pd
import bento_mdf


'''def buildLoadSheets(mdf):
    # TODO: Move to the version in crdclib 
    loadsheets = {}
    nodes = mdf.model.nodes
    for node in nodes:
        nodeprops = mdf.model.nodes[node].props
        nodelist = []
        for prop in nodeprops:
            if 'Template' in mdf.model.props[(node, prop)].tags:
                if mdf.model.props[(node,prop)].tags['Template'].get_attr_dict()['value'] != 'No':
                    nodelist.append(prop)
            else:
                nodelist.append(prop)
        # Now need to add the relationships.
        srcedges = mdf.model.edges_by_src(mdf.model.nodes[node])
        for srcedge in srcedges:
            # Need to find the destination node:
            dstnode = srcedge.dst.handle
            #Now get the properties for that node
            dstprops = mdf.model.nodes[dstnode].props
            reqlist = []
            for dstprop in dstprops:
                if mdf.model.props[(dstnode, dstprop)].get_attr_dict()['is_key'] == 'True':
                    reqlist.append(dstnode+'.'+dstprop)
            nodelist.extend(reqlist)

        load_df = pd.DataFrame(columns=nodelist)
        loadsheets[node] = load_df
    return loadsheets'''



def buildSourceSheets(xlfilepath):

    sourcesheets = {}

    xlfile = pd.ExcelFile(xlfilepath)
    for node in xlfile.sheet_names:
        if node not in ['Dictionary', 'Terms and Value Sets', 'README adn INSTRUCTIONS']:
            temp_df = pd.read_excel(xlfilepath, sheet_name=node, engine='openpyxl')
            if 'type' in temp_df.columns.tolist():
                # Clear out the type column, we'll add it back later
                temp_df = temp_df.drop('type', axis=1)
                #Drop all rows that only have NaN
                temp_df = temp_df.dropna(axis=0, how='all')
            sourcesheets[node] = temp_df
    return sourcesheets



def loadTheseSheets(mapping_info,loadsheets):
    #Dictionary of dictionary
    for node, movedvalues in mapping_info.items():
        temploadsheet_df = loadsheets[node]
        temploadsheet_df.loc[len(temploadsheet_df)] = movedvalues
    return loadsheets


def valueCheck(targetvalue, source_value_mapping_df):
    temp_df = source_value_mapping_df.query('lift_from_pv == @targetvalue')
    temp_df.reset_index(drop=True, inplace=True)
    if not temp_df.empty:
        targetvalue = temp_df.loc[0]['lift_from_pv']
    return targetvalue

def loadTransformInfo(node, prop, data, transform_info):
    if node in transform_info:
        temp = transform_info[node]
        temp[prop] = data
        transform_info[node] = temp
    else:
        transform_info[node] = {prop:data}
    return transform_info


def populateEdges(datarow, targetprops, transform_info, node, complextransforms):
     #In the target sheet, need to look for node.property fields
    # Then need to query the data row to see if node.property is there
    # If not, need to get the node source sheet and query for the row to get the property part of node.property
    edgefields = []
    complex_merge_nodes = []
    for entry in complextransforms['Merge']:
        complex_merge_nodes.append(list(entry['To'].keys())[0])

    for targetprop in targetprops:
        if "." in targetprop:
            edgefields.append(targetprop)

    for edgefield in edgefields:
        if edgefield in datarow:
            temp = edgefield.split(".")[0]
            targetnode = temp[0]
            targetprop = temp[1]
            if targetnode in complex_merge_nodes:
                for entry in complextransforms['Merge']:
                    if targetnode == list(entry['To'].keys())[0]:
                        mergechar = entry['Method']
                        merdged_data = None
                        for fromentry in entry['From']:
                            for fromnode, fromprop in fromentry.items():
                                if fromnode == node:
                                    if merdged_data == None:
                                        merdged_data = datarow[fromprop]
                                    else:
                                        merdged_data = merdged_data+mergechar+datarow[fromprop]
                                else:
                                    # need to get the fromnode datasheet
                                    print("Doing the else thing")
                        transform_info = loadTransformInfo(targetnode, targetprop, merdged_data, transform_info)
    return transform_info





'''def populateEdges(datarow, targetprops, transform_info, node):
    #In the target sheet, need to look for node.property fields
    # Then need to query the data row to see if node.property is there
    # If not, need to get the node source sheet and query for the row to get the property part of node.property
    edgefields = []
    for targetprop in targetprops:
        if "." in targetprop:
            edgefields.append(targetprop)
    for edgefield in edgefields:
        #Check to see if edgefield happens to be in datarow
        if edgefield in datarow:
            if node in transform_info:
                temp = transform_info[node]
                temp[edgefield] = datarow[edgefield]
                transform_info[node] = temp
            else:
                transform_info[node] = {edgefield:datarow[edgefield]}

        else:
            #We have to query the data from a different datafram to get the value
            print(f"we are screwed for {edgefield} in {node}")
            edgelist = edgefield.split(".")
            originalnode = edgelist[0]
            originalfield = edgelist[1]
            #source_df = sourcesheet_df[originalnode]
            if originalfield in source_df.columns.tolist():
                print(f"Found {originalfield} in source_df {originalnode}")
        
    return transform_info'''



def propertyMoveIt(row, fullprops, mappedprops, sourcenode_mapping_df, transform_info, source_value_mapping_df, loadsheets, complextransforms):
    #print(f"In propertyMoveIt transform_info: {transform_info} of type {type(transform_info)}")
    for prop in fullprops:
        if prop in mappedprops:
            target_df = sourcenode_mapping_df.query('lift_from_prop == @prop')
            #print(f"Starting For target_Df transform_info: {transform_info} of type {type(transform_info)}")
            for index, targetrow in target_df.iterrows():
                #print(f"In For target_df transform_info: {transform_info} of type {type(transform_info)}")
                targetnode = targetrow['lift_to_node']
                targetprop = targetrow['lift_to_prop']
                targetvalue = row[prop]
                targetvalue = valueCheck(targetvalue, source_value_mapping_df)
                #print(f"In propertyMoveIt transform_info: {transform_info} of type {type(transform_info)}")
                if targetnode in transform_info:
                    temp = transform_info[targetnode]
                    temp[targetprop] =targetvalue
                    transform_info[targetnode] = temp
                else:
                    #print(f"Startin Else transform_info: {transform_info} of type {type(transform_info)}")
                    transform_info[targetnode] = {targetprop:targetvalue}
                    #print(f"After Else transform_info: {transform_info} of type {type(transform_info)}")
                # Now to fill in the edges
                targetproplist = loadsheets[targetnode].columns.tolist()
                # THE ONLY SANE WAY TO GET RELATIONSHIPS IN PLACE IS VIA MANUAL MAPPING
                transform_info = populateEdges(row, targetproplist, transform_info, targetnode, complextransforms)
    #print(f"Returning from propertyMoveIt transform_info: {transform_info} of type {type(transform_info)}")
    return transform_info





def iWantToMoveIt(sourcesheet_df, sourcenode_mapping_df, sourcevalue_mapping_df,loadsheets, complextransforms, verbose = 0):
    # For each row, need to look at the colum header, find out if it was mapped(properties), then look if the PV was mapped
    mappedprops = sourcenode_mapping_df['lift_from_prop'].unique().tolist()
    fullprops = sourcesheet_df.columns.unique().tolist()

    # Need to move row by row.  Need the target node, target property, and source value.
    for index, row, in sourcesheet_df.iterrows():
        #Create a holder for the row transformation info
        transform_info = {}
        if verbose >= 2:
            print("Calling propertyMoveIt")
            print(f"transform_info: {transform_info}")
        transform_info = propertyMoveIt(row, fullprops, mappedprops, sourcenode_mapping_df, transform_info, sourcevalue_mapping_df, loadsheets, complextransforms)
        if verbose >= 2:
            print("Calling loadTheseSheets")
        loadsheets = loadTheseSheets(transform_info, loadsheets)
    return loadsheets


def printThisMess(loadsheets, targetdir):
    for node, loadsheet in loadsheets.items():
        if loadsheet.empty:
            print(f"Loadsheet for {node} is empty")
        elif not loadsheet.empty:
            # Add the type column to the dataframe
            loadsheet.insert(0,'type', node)
            filename = f"GC_{node}.tsv"
            loadsheet.to_csv(targetdir+filename, sep="\t", index=False)


def main(args):
    #Configuration parsing
    if args.verbose >= 1:
        print("Reading configs")
    configs = crdclib.readYAML(args.configfile)

    #Read mapping file
    if args.verbose >= 1:
        print("Creating Value and Property Map Dataframes")
    valuemap_df = pd.read_csv(configs['value_mapping_file'], sep="\t")
    propertymap_df = pd.read_csv(configs['property_mapping_file'], sep='\t')

    #Get the lift_to model and make a load sheet collection
    if args.verbose >= 1:
        print("Reading the data model and buildign loadsheets")
    lift_to_mdf = bento_mdf.MDF(*configs['lift_to_model_files'])
    #loadsheets = buildLoadSheets(lift_to_mdf)
    loadsheets = crdclib.mdfBuildLoadSheets(lift_to_mdf)

    #Get complex transforms
    complextransforms = crdclib.readYAML(configs['complex_mapping_file'])

   
    #Load up the CCDI Excel sheets
    if args.verbose >= 1:
        print("Reading Excel workbook and making source sheets")
    sourcesheets = buildSourceSheets(configs['ccdi_excel'])

    #Now comes the fun part
    #Look for matching nodes
    if args.verbose >= 1:
        print("Creating starting nodes and mapped nodes")
    starting_sourcenodes = list(sourcesheets.keys())
    mapped_sourcenodes = propertymap_df['lift_from_node'].unique().tolist()
    #print(f"Starting nodes:\n{starting_sourcenodes}\nMapped nodes:\n{mapped_sourcenodes}")

    if args.verbose >= 1:
        print("Creating source node and value mapping dataframes")
    for sourcenode in starting_sourcenodes:
        if sourcenode in mapped_sourcenodes:
            # Get the dataframe of mappings for just the source node.
            sourcenode_mapping_df = propertymap_df.query('lift_from_node == @sourcenode')
            sourcevalue_mapping_df = valuemap_df.query('lift_from_node == @sourcenode')
            # Now pass the stating data, mapping df, and load sheets for transformation.
            if args.verbose >= 1:
                print(f"Moving the data for source {sourcenode}")
            loadsheets = iWantToMoveIt(sourcesheets[sourcenode],sourcenode_mapping_df, sourcevalue_mapping_df, loadsheets, complextransforms, args.verbose)

    printThisMess(loadsheets, configs['outputdir'])
            


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument('-v', '--verbose', action='count', default=0, help=("Verbosity: -v main section -vv subroutine messages -vvv data returned shown"))

    args = parser.parse_args()

    main(args)