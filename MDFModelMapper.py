import bento_mdf
import pandas as pd
import numpy as np
import argparse
import yaml
import requests

#TODO:  Build in an optional synonym mapping.  STS returns synonyms for each CDE.

def stsPVQuery(id, version, model=False, verbose=False):
    if id == 'CDS':
        version = '9.0.0'
    if verbose:
        print(f"stsPVQuery for ID: {id} and version {version}")
    
    #url = f"https://sts-dev.ctos-data-team.org/v1/terms/cde-pvs/{cdeid}/{cdeversion}/pvs"
    if model:
        url = f"https://sts-dev.cancer.gov/v1/terms/model-pvs/{id}/{version}/pvs"
    else:
        url = f"https://sts.cancer.gov/v1/terms/cde-pvs/{id}/{version}/pvs"
    headers = {'accept': 'application/json'}
    
    if verbose:
        print(f"URL: {url}")
    try:
        result = requests.get(url = url, headers = headers)

        if result.status_code == 200:
            return result.json()
        else:
            print(f"Error: {result.status_code}\n{result.content}")
            return None
    except requests.exceptions.HTTPError as e:
        print("HTTP Error: {e}")
        return None




'''def conceptCodeQuery(conceptcode, verbose):
    # Searches caDSR with the concept code and returns CDE info
    url = f"https://cadsrapi.cancer.gov/rad/NCIAPI/1.0/api/DataElements/Concept?conceptCode={conceptcode}&headerOnly=true"
    headers = {'accept': 'application/json'}
    if verbose:
        print(f"URL: {url}")
    try:
        result = requests.get(url = url, headers = headers)

        if result.status_code == 200:
            return result.json()
        else:
            print(f"Error: {result.status_code}\n{result.content}")
            return None
    except requests.exceptions.HTTPError as e:
        print("HTTP Error: {e}")
        return None'''




def readYAML(yamlfile):
    with open(yamlfile) as f:
        yamljson = yaml.load(f, Loader=yaml.FullLoader)
    return yamljson

def getCDEID(props, prop, verbose= False):
    if verbose:
        print(f"getCDEID for Props: {props} and Prop: {prop}")
    if props[prop].concept is not None:
        workingterms = props[prop].concept.terms.values()
        for workingtermobj in workingterms:
            workingterm = workingtermobj.get_attr_dict()
            return workingterm['origin_id'], workingterm['origin_version']
    else:
        return None, None

'''def CDEDictionary(props, verbose = False):
    # id_dict is a dictionary with the CDEID as the key and a list of dicitonary of property name: node name as the value
    # CDEID:[{property name: node name}]
    if verbose:
        print(f"CDEDictionary for Props {props}")
    id_dict = {}
    for prop in props:
        if props[prop].concept is not None:
            cdeid, cdeversion = getCDEID(props, prop)
            if cdeid not in id_dict.keys():
                id_dict[cdeid]  = [{prop[1]:prop[0]}]
            else:
                templist = id_dict[cdeid]
                templist.append({prop[1]:prop[0]})
                id_dict[cdeid] = templist
    return id_dict'''

def CDEDataFrame(props, verbose = False):
    # Using a dataframe instead
    if verbose:
        print(f"CDEDataFrame for Props {props}")
    columns = ["cde_id", "cde_version", "property_name", "node_name"]
    final_df = pd.DataFrame(columns=columns)
    for prop in props:
        if props[prop].concept is not None:
            cdeid, cdeversion = getCDEID(props, prop)
            final_df.loc[len(final_df)] = {"cde_id": cdeid, "cde_version":cdeversion, "property_name": prop[1], "node_name": prop[0]}
    return final_df

'''def PropStringDictionary(props, verbose= False):
    # Creates a dictionary with property names as key and a dictionary of {node:CDEID} as the value
    # {PropName:{NodeName:CDEID}}
    if verbose:
        print(f"StringDcitionary for Props {props}")
    prop_dict = {}
    for prop in props:
        if props[prop].concept is not None:
            cdeid = getCDEID(props, prop)
        else:
            cdeid = 'None'
        prop_dict[prop[1]] = {prop[0]:cdeid}
    return prop_dict'''
                
'''def CDEInfo(mdf, verbose= False):
    # Returns a dictionary of cdeid:cde version
    if verbose:
        print(f"CDEInfo for {mdf.model.handle}")
    props = mdf.model.props
    final = {}
    for prop in props:
        if props[prop].concept is not None:
            workingterms = props[prop].concept.terms.values()
            for workingtermobj in workingterms:
                workingterm = workingtermobj.get_attr_dict()
                #print(workingterm)
                if 'origin_version' in workingterm:
                    final[workingterm['origin_id']] = workingterm['origin_version']
                else:
                    final[workingterm['origin_id']] = '1.00'
    return final'''
                

def PVDictionary(cdeid, cdeversion,  verbose=False):
    #Returns a dictionary of {concept code: PV term}.
    if verbose:
        print(f"PVDictionary for CDE ID {cdeid} and version {cdeversion}")
    final = {}
    #synonyms = {}
    cdejson = stsPVQuery(cdeid, cdeversion, False, verbose)
    if verbose:
        print(cdejson)
    # If multiple models have different names for a CDE it might come back as a list
    if cdejson is not None:
        if type(cdejson['CDECode']) is list:
            if len(cdejson['permissibleValues'][0]) > 0:
                for pv in cdejson['permissibleValues'][0]:
                    final[pv['ncit_concept_code']] = pv['value']
            else:
                final = None
        elif len(cdejson['permissibleValues']) > 0:
            for pv in cdejson['permissibleValues']:
                final[pv['ncit_concept_code']] = pv['value']
        else:
            final = None
    else:
        final = None
    return final


def SynonymDictionary(modelhandle, modelversion, verbose=False):
    # Returns a dicitonary of {concept code: [synonyms]} 
    # Some people like to put a "v" in front of the model version.  This nukes that
    modelversion = modelversion.replace("v", "")
    if verbose:
        print(f"SynonymDictionary for {modelhandle} version {modelversion}")
    pvjson = stsPVQuery(modelhandle, modelversion, True, verbose)
    if verbose:
        print(pvjson)
    final = {}
    
    if pvjson is not None:
        for cdelist in pvjson['permissibleValues']:
            for cde in cdelist:
                if verbose:
                    print(cde)
                if cde['ncit_concept_code'] is not None:
                    if len(cde['synonyms']) > 0:
                        final[cde['ncit_concept_code']] = cde['synonyms']
    else:
        pvjson = None
    return final



'''def conceptCodeCDEMapping(conceptcode, mapping_string, verbose):
    # Returns {cdeid:{node:property}}
    # Mapping string should be {property name:{node name:cde id}}
    #
    # Need a quck remap of mapping string to {cdeid:{node, property}}
    redone = {}
    final = {}
    if verbose:
        print(f"CDEMapping with {conceptcode}")
    for propname, info in mapping_string.items():
        for node, cdeid in info.items():
             # ACTUAL: Redone: {('11444542', '2.00'): {'program': 'program_name'}, 'None': {'version': 'description'}, ('11459801', '2.00'): {'program': 'program_short_name'}
            redone[cdeid] = {node:propname}
    print(f"Redone: {redone}\n")
    ccjson = conceptCodeQuery(conceptcode, verbose)
    if verbose:
        print(f"conceptCodeQuery Result: {ccjson}")
    if ccjson is not None:
        for dataelement in ccjson['DataElements']:
            if dataelement['publicId'] in redone:
                final[dataelement['publicId']] = redone[dataelement['publicId']]
                print(f"Final dict: {final}")
            else:
                print(f"Data Element: {dataelement['publicId']} is not found in redone")
    return final'''
            
        
'''
def doCDEPropertyMapping(lift_from_model, lift_to_model, lift_from_dict, lift_to_dict, lift_from_cdeinfo, lift_to_cdeinfo, mapped_df, verbose=False):
    # Map Properties by CDE ID
    # Lift_from and Lift_to dict  CDEID:[{property name: node name}]
    if verbose:
        print(f"doCDEPropertyMapping")
    for cdeid, propinfolist in lift_from_dict.items():
        #NOTE:  propinfolist is a list of dictionary [{CDEID:{property name: node name}}]
        if cdeid in lift_to_dict:
            for propinfo in propinfolist:
                if len(lift_to_dict[cdeid]) == 1:
                    if verbose:
                        print(f"Mapping CDE ID {cdeid}")
                        if len(lift_to_dict[cdeid][0].keys()[0]) <= 1:
                            print(f"Start: {lift_to_dict[cdeid][0]}")
                            print(f"Node: {lift_to_dict[cdeid][0].keys()}")
                            print(f"Values: {lift_to_dict[cdeid][0].values()}")
                    mapped_df.loc[len(mapped_df)] = {'lift_from_node': list(propinfo.values())[0],
                                                    'lift_from_prop': list(propinfo.keys())[0],
                                                    'lift_from_cdeID': cdeid,
                                                    'lift_from_cdeVersion': lift_from_cdeinfo[cdeid],
                                                    'lift_from_model': lift_from_model.model.handle,
                                                    'lift_from_version': lift_from_model.model.version,
                                                    'lift_to_node': list(lift_to_dict[cdeid][0].values())[0],
                                                    'lift_to_prop': list(lift_to_dict[cdeid][0].keys())[0],
                                                    'lift_to_cdeID': cdeid,
                                                    'lift_to_cdeVersion': lift_to_cdeinfo[cdeid],
                                                    'lift_to_model': lift_to_model.model.handle,
                                                    'lift_to_version': lift_to_model.model.version,
                                                    'mapping_type': 'CDE'}
                else:
                    for propentry in lift_to_dict[cdeid]:
                        for to_key, to_values in propentry.items():
                            mapped_df.loc[len(mapped_df)] = {'lift_from_node': list(propinfo.values())[0],
                                                            'lift_from_prop': list(propinfo.keys())[0],
                                                            'lift_from_cdeID': cdeid,
                                                            'lift_from_cdeVersion': lift_from_cdeinfo[cdeid],
                                                            'lift_from_model': lift_from_model.model.handle,
                                                            'lift_from_version': lift_from_model.model.version,
                                                            'lift_to_node': list(to_values)[0],
                                                            'lift_to_prop': list(to_key)[0],
                                                            'lift_to_cdeID': cdeid,
                                                            'lift_to_cdeVersion': lift_to_cdeinfo[cdeid],
                                                            'lift_to_model': lift_to_model.model.handle,
                                                            'lift_to_version': lift_to_model.model.version,
                                                            'mapping_type': 'CDE'}
    return mapped_df'''


def doCDEPropertyMapping2(from_df, to_df, mapped_df, lift_from_model, lift_to_model,verbose=False):
    for index, row in from_df.iterrows():
        if row['cde_id'] in to_df['cde_id'].unique():
            cdeid = row['cde_id']
            to_rows_df = to_df.query('cde_id == @cdeid')
            for to_index, to_row in to_rows_df.iterrows():
                mapped_df.loc[len(mapped_df)] = {'lift_from_node': row['node_name'],
                                                    'lift_from_prop': row['property_name'],
                                                    'lift_from_cdeID': row['cde_id'],
                                                    'lift_from_cdeVersion': row['cde_version'],
                                                    'lift_from_model': lift_from_model.model.handle,
                                                    'lift_from_version': lift_from_model.model.version,
                                                    'lift_to_node': to_row['node_name'],
                                                    'lift_to_prop': to_row['property_name'],
                                                    'lift_to_cdeID': to_row['cde_id'],
                                                    'lift_to_cdeVersion': to_row['cde_version'],
                                                    'lift_to_model': lift_to_model.model.handle,
                                                    'lift_to_version': lift_to_model.model.version,
                                                    'mapping_type': 'CDE'}
    return mapped_df
            
'''
def doStringPropertyMapping(lift_from_model, lift_to_model, mapped_df, verbose= False):
    # Map Properties by string matching
    if verbose:
        print("doStringPropertyMapping")
    lift_from_string = PropStringDictionary(lift_from_model.model.props)
    lift_to_string = PropStringDictionary(lift_to_model.model.props)
        
    for propname, nodeinfo in lift_from_string.items():
        # Only map if not already mapped by CDE
        if propname not in mapped_df['lift_from_prop'].unique():
            if propname in lift_to_string:
                if verbose:
                    print(f"String mapping {propname}")
                mapped_df.loc[len(mapped_df)] = {'lift_from_node': list(nodeinfo.keys())[0],
                                                'lift_from_prop': propname,
                                                'lift_from_cdeID': list(nodeinfo.values())[0],
                                                'lift_from_model': lift_from_model.model.handle ,
                                                'lift_from_version': lift_from_model.model.version,
                                                'lift_to_node': list(lift_to_string[propname].keys())[0],
                                                'lift_to_prop': propname,
                                                'lift_to_cdeID': list(lift_to_string[propname].values())[0],
                                                'lift_to_model': lift_to_model.model.handle,
                                                'lift_to_version': lift_to_model.model.version,
                                                'mapping_type': 'String'}
    return mapped_df'''


def doStringPropertyMapping2(from_df, to_df, mapped_df, lift_from_model, lift_to_model, verbose=False):
    # Map properties by sting matching
    for index, row in from_df.iterrows():
        if row['property_name'] not in mapped_df['lift_from_prop'].unique():
            query = row['property_name']
            to_row_df = to_df.query('property_name == @query')
            for index, to_row in to_row_df.iterrows():
                if verbose:
                    print(f"String mapping {to_row['property_name']}")
                mapped_df.loc[len(mapped_df)] = {'lift_from_node': row['node_name'],
                                                'lift_from_prop': row['property_name'],
                                                'lift_from_cdeID': row['cde_id'],
                                                'lift_from_cdeVersion': row['cde_version'],
                                                'lift_from_model': lift_from_model.model.handle ,
                                                'lift_from_version': lift_from_model.model.version,
                                                'lift_to_node': to_row['node_name'],
                                                'lift_to_prop': to_row['property_name'],
                                                'lift_to_cdeID': to_row['cde_id'],
                                                'lift_to_cdeVersion': to_row['cde_version'],
                                                'lift_to_model': lift_to_model.model.handle,
                                                'lift_to_version': lift_to_model.model.version,
                                                'mapping_type': 'String'}
    return mapped_df
                


'''
def doConceptCodeValueMapping(lift_from_cdeinfo, lift_to_cdeinfo, lift_from_model, lift_to_model, lift_from_dict, lift_to_dict, mapped_pv_df, verbose = False):
    # Will match PVs using concept codes for properties that have a CDE
    
    # Step 1 - get from and to properties.  lift_from_dict, lift_to_dict  {CDEID:[{property name: node name}]}
    #          Passed in as parameters for the subrouting
    # Step 2 - If from property is in to property, continue.  There's a match and we can compare PVs
    #          lift_from_dict key (CDEID) in lift_to_dict.keys()
    # Step 3 - Get from and to PV lists and concept codes
    #          lift_from_cdeinfo, lift_to_cdeinfo have {cdeid:cdeversion}.  Those can be turned into PV lists with PVDictionary.  {concept code: PV term}
    # Step 4 - For each from concept code, look for to concept code.  Add to df if match
    #          compare from concept code to to concept codes.
    
    if verbose:
        print("doConceptCodeValueMapping")
    for from_cdeid, propinfolist in lift_from_dict.items():
        #NOTE:  propinfolist is a list of dictionary [{property name: node name}]
        # Check if the CDE exists in the target.  No point in mapping to something that doesn't exist.
        if verbose:
            print(f" Checking CDE ID: {from_cdeid}")
        if from_cdeid in lift_to_dict.keys():
            if verbose:
                print(f"CDE {from_cdeid} in lift_to_dict")
            for propinfo in propinfolist:
                for propname, nodename in propinfo.items():
                    # Not sure why I need to check if the id is in lift_to_cdeinfo.  Safety valve?
                    if from_cdeid in lift_to_cdeinfo:
                        pv_from_codes = PVDictionary(from_cdeid, lift_from_cdeinfo[from_cdeid], verbose)
                        pv_to_code = PVDictionary(from_cdeid, lift_to_cdeinfo[from_cdeid], verbose)

                        # No sense doing anything if pv_from_codes is empty
                        if pv_from_codes is not None:
                            for conceptcode, pvtext in pv_from_codes.items():
                                if conceptcode is not None:
                                    if conceptcode in pv_to_code:
                                        if 'C' not in conceptcode:
                                            print(f"From Concept Code: {conceptcode}\tText: {pvtext}")
                                            print(f"To Concept Record: {pv_to_code[conceptcode]}")
                                        if verbose:
                                            print(f"Concept code mapping CDE ID {from_cdeid}")
                                        mapped_pv_df.loc[len(mapped_pv_df)] = {
                                            'lift_from_node': nodename,
                                            'lift_from_prop': propname,
                                            'lift_from_cdeID': from_cdeid,
                                            'lift_from_cdeVersion': lift_from_cdeinfo[from_cdeid],
                                            'lift_from_pv': pvtext,
                                            'lift_from_conceptCode': conceptcode,
                                            'lift_from_model': lift_from_model.model.handle,
                                            'lift_from_version': lift_from_model.model.version,
                                            'lift_to_node': list(lift_to_dict[from_cdeid][0].values())[0],
                                            'lift_to_prop': list(lift_to_dict[from_cdeid][0].keys())[0],
                                            'lift_to_cdeID': from_cdeid,
                                            'lift_to_cdeVersion': lift_to_cdeinfo[from_cdeid],
                                            'lift_to_pv': pv_to_code[conceptcode],
                                            'lift_to_conceptCode': conceptcode,
                                            'lift_to_model': lift_to_model.handle,
                                            'lift_to_version': lift_to_model.version,
                                            'mapping_type': 'Concept Code Mapping'
                                        }
    return mapped_pv_df'''


def conceptCodifier(df, verbose=False):
    # {concept code: PV term}
    columns = ["cde_id", "cde_version", "property_name", "node_name", "permissible_value", "concept_code"]
    new_df = pd.DataFrame(columns=columns)
    for index, row in df.iterrows():
        pvstuff = PVDictionary(row['cde_id'], row['cde_version'], verbose)
        if pvstuff is not None:
            for concept_code, pv in pvstuff.items():
                new_df.loc[len(new_df)] = {
                    "cde_id": row['cde_id'],
                    "cde_version": row['cde_version'],
                    "property_name": row['property_name'],
                    "node_name": row['node_name'],
                    "permissible_value": pv,
                    "concept_code": concept_code
                }
        else:
            new_df.loc[len(new_df)] = {
                    "cde_id": row['cde_id'],
                    "cde_version": row['cde_version'],
                    "property_name": row['property_name'],
                    "node_name": row['node_name'],
                    "permissible_value": np.nan,
                    "concept_code": np.nan
            }
    return new_df 
        

def doConceptCodeValueMapping2(from_df, to_df, lift_from_model, lift_to_model, mapped_pv_df, verbose = False):
    # Will match PVs using concept codes for properties that have a CDE
    # NOTE: the from_df and to_df have been modified to have pvs and concept codes
    
    # Step 1 - get from and to properties.  
    #          In from, to_df['property_name']
    #          Passed in as parameters for the subrouting
    # Step 2 - If from property is in to property, continue.  There's a match and we can compare PVs
    #          CDEs in row['cde_id']
    # Step 3 - Get from and to PV lists and concept codes
    #          lift_from_cdeinfo, lift_to_cdeinfo have {cdeid:cdeversion}.  Those can be turned into PV lists with PVDictionary.  {concept code: PV term}
    # Step 4 - For each from concept code, look for to concept code.  Add to df if match
    #          compare from concept code to to concept codes.
    if verbose:
        print("doConceptCodeValueMapping")
    for index, row in from_df.iterrows():
        if row['concept_code'] in to_df['concept_code'].unique():
            cc = row['concept_code']
            temp_df = to_df.query('concept_code ==@cc')
            for to_index, to_row in temp_df.iterrows():
                mapped_pv_df.loc[len(mapped_pv_df)] = {
                                            'lift_from_node': row['node_name'],
                                            'lift_from_prop': row['property_name'],
                                            'lift_from_cdeID': row['cde_id'],
                                            'lift_from_cdeVersion': row['cde_version'],
                                            'lift_from_pv': row['permissible_value'],
                                            'lift_from_conceptCode': row['concept_code'],
                                            'lift_from_model': lift_from_model.model.handle,
                                            'lift_from_version': lift_from_model.model.version,
                                            'lift_to_node': to_row['node_name'],
                                            'lift_to_prop': to_row['property_name'],
                                            'lift_to_cdeID': to_row['cde_id'],
                                            'lift_to_cdeVersion': to_row['cde_version'],
                                            'lift_to_pv': to_row['permissible_value'],
                                            'lift_to_conceptCode': to_row['concept_code'],
                                            'lift_to_model': lift_to_model.handle,
                                            'lift_to_version': lift_to_model.version,
                                            'mapping_type': 'Concept Code Mapping'
                                        }
    return mapped_pv_df

'''
def doSynonymValueMapping(lift_from_model, lift_to_model, mapped_pv_df, verbose= False):
    # Matches PV via string matching to known synonyms
    #
    # Step 1: get from and to property names and synonyms
    #         from PropStringDictionary {property name:{node name:cde id}}
    #           from SynonymDictionary {concept code:[synonyms]}
    # Step 2: For each from property, get pv list
    #           CDE ID and version from getCDEID
    #           Use PVDictionary, needs cdeid and version   {concept code: PV term}
    # Step 3: For each PV, check if it was already mapped by concept code
    # Step 4: For anything not mapped by concept code, check agasint all to synonyms
    # Step 5 If match found, add to dataframe
    if verbose:
        print("doSynonymValueMapping")
    # {PropName:{NodeName:CDEID}}
    lift_from_string = PropStringDictionary(lift_from_model.model.props, verbose)
    lift_to_string = PropStringDictionary(lift_to_model.model.props, verbose)
     # {concept code: [synonyms]}
    lift_to_synonyms = SynonymDictionary(lift_to_model.model.handle, lift_to_model.model.version, verbose)
    
    for propname, propinfo in lift_from_string.items():
        if verbose:
            print(f"Synonym check for property {propname}")
        nodename = list(propinfo.keys())[0]
        cdeid, cdeversion = getCDEID(lift_from_model.model.props, (nodename,propname), verbose)
        from_pvs = PVDictionary(cdeid, cdeversion, verbose) # {concept code: PV term}
        if from_pvs is not None:
            for conceptCode, pvText in from_pvs.items():
                if pvText not in mapped_pv_df['lift_from_pv'].unique():
                    if verbose:
                        print(f"PV {pvText} is not mapped by concept code")
                    for synConceptCode, synList in lift_to_synonyms.items():
                        if pvText in synList:
                            if verbose:
                                print(f"{pvText} synonym found in {synConceptCode}")
                                # Need the to node, property and cdeID.
                                # 
                                #{cdeid:{node, property}}
                            ccinfo = conceptCodeCDEMapping(conceptCode, lift_to_string, verbose)
                            print(f"CCinfo: {ccinfo}")
                            for to_cdeid, info in ccinfo.items():
                                print("for to_cdeid")
                                for to_node, to_prop in info.items():
                                    print("to_node")
                                    if verbose:
                                        print(f"Adding to DF: to_cdeid: {cdeid}\tto_node: {to_node}\tto_prop: {to_prop}")
                                    mapped_pv_df.loc[len(mapped_pv_df)] = {'lift_from_node': list(propinfo.values())[0],
                                                                    'lift_from_prop': propname,
                                                                    'lift_from_cdeID': cdeid,
                                                                    'lift_from_cdeVersion':cdeversion,
                                                                    'lift_from_pv': pvText,
                                                                    'lift_from_conceptCode': conceptCode,
                                                                    'lift_from_model': lift_from_model.model.handle,
                                                                    'lift_from_version': lift_from_model.model.version,
                                                                    'lift_to_node': to_node,
                                                                    'lift_to_prop': to_prop,
                                                                    'lift_to_cdeID': to_cdeid,
                                                                    'lift_to_cdeVersion': 'NA',
                                                                    'lift_to_pv': pvText,
                                                                    'lift_to_conceptCode': synConceptCode,
                                                                    'lift_to_model': lift_to_model.handle,
                                                                    'lift_to_version': lift_to_model.version,
                                                                    'mapping_type': 'Synonym Mapping'}
                                    mapped_pv_df.to_csv('./df_log.csv', sep="\t", index=False)
                else:
                    if verbose:
                        print(f"Skipping. PV {pvText} was mapped by concept code")
                
    return mapped_pv_df'''

def doSynonymValueMapping2(from_df, to_df, mapped_pv_df, lift_from_model, lift_to_model, verbose):
    lift_to_synonyms = SynonymDictionary(lift_to_model.model.handle, lift_to_model.model.version, verbose)
    # {concept code: [synonyms]} 
    for index, row in from_df.iterrows():
        if row['concept_code'] not in mapped_pv_df['lift_from_conceptCode'].unique():
            if row['concept_code'] in lift_to_synonyms.keys():
                for to_cc, synlist in lift_to_synonyms.items():
                    if row['permissible_value'] in synlist:
                        query_pv = row['permissible_value']
                        temp_df = to_df.query('permissible_value == @query_pv')
                        for temp_index, temp_row in temp_df.iterrows():
                            mapped_pv_df.loc[len(mapped_pv_df)] = {'lift_from_node':row['node_name'],
                                                                    'lift_from_prop': row['property_name'],
                                                                    'lift_from_cdeID': row['cde_id'],
                                                                    'lift_from_cdeVersion': row['cde_version'],
                                                                    'lift_from_pv': row['permissible_value'],
                                                                    'lift_from_conceptCode': row['concept_code'],
                                                                    'lift_from_model': lift_from_model.model.handle,
                                                                    'lift_from_version': lift_from_model.model.version,
                                                                    'lift_to_node': temp_row['node_name'],
                                                                    'lift_to_prop': temp_row['property_name'],
                                                                    'lift_to_cdeID': temp_row['cde_id'],
                                                                    'lift_to_cdeVersion': temp_row['cde_version'],
                                                                    'lift_to_pv': temp_row['permissible_value'],
                                                                    'lift_to_conceptCode': temp_row['concept_code'],
                                                                    'lift_to_model': lift_to_model.handle,
                                                                    'lift_to_version': lift_to_model.version,
                                                                    'mapping_type': 'Synonym Mapping'}
    return mapped_pv_df


def main(args):
    
    # Mapping order
    # 1 - Property-Property by CDE match
    # 2 - Property-Property by String match
    # 3 - Value-Value by concept code match
    # 4 - Value-Value by synonym
    
    configs = readYAML(args.configfile)
    
    mapped_df_headers = ['lift_from_node',
                         'lift_from_prop',
                         'lift_from_cdeID',
                         'lift_from_cdeVersion',
                         'lift_from_model',
                         'lift_from_version',
                         'lift_to_node',
                         'lift_to_prop',
                         'lift_to_cdeID',
                         'lift_to_cdeVersion',
                         'lift_to_model',
                         'lift_to_version',
                         'mapping_type']
    
    pv_df_mapping_headers = ['lift_from_node',
                         'lift_from_prop',
                         'lift_from_cdeID',
                         'lift_from_cdeVersion',
                         'lift_from_pv',
                         'lift_from_conceptCode',
                         'lift_from_model',
                         'lift_from_version',
                         'lift_to_node',
                         'lift_to_prop',
                         'lift_to_cdeID',
                         'lift_to_cdeVersion',
                         'lift_to_pv',
                         'lift_to_conceptCode',
                         'lift_to_model',
                         'lift_to_version',
                         'mapping_type']
    
    mapped_df = pd.DataFrame(columns=mapped_df_headers)
    mapped_pv_df = pd.DataFrame(columns=pv_df_mapping_headers)
    
    lift_from_model = bento_mdf.MDF(*configs['lift_from_model_files'])
    lift_to_model = bento_mdf.MDF(*configs['lift_to_model_files'])
    
    #lift_from_dict = CDEDictionary(lift_from_model.model.props)
    #lift_to_dict = CDEDictionary(lift_to_model.model.props)
    
    lift_from_dict_df = CDEDataFrame(lift_from_model.model.props)
    lift_to_dict_df = CDEDataFrame(lift_to_model.model.props)
    
    #print("Lift_to_dict Values:\n")
    #for key, values in lift_to_dict.items():
    #    for value in values:
    #        print(f"{value}")
    
    #lift_from_cdeinfo = CDEInfo(lift_from_model)
    #lift_to_cdeinfo = CDEInfo(lift_to_model)
    
    
    #
    #  Property Mapping
    #
    
    
    if args.verbose:
        print("Starting Property CDE based mapping")
    
    # Do CDE based Property mapping if requested
    if configs['cde_mapping']:
        if args.verbose:
            print("Starting CDE Mapping")
        #mapped_df = doCDEPropertyMapping(lift_from_model, lift_to_model, lift_from_dict, lift_to_dict, lift_to_cdeinfo, lift_to_cdeinfo, mapped_df, args.verbose)
        #mapped_df = doCDEPropertyMapping(lift_from_model, lift_to_model, lift_from_dict, lift_to_dict, lift_from_cdeinfo, lift_to_cdeinfo, mapped_df, False)
        mapped_df = doCDEPropertyMapping2(lift_from_dict_df, lift_to_dict_df, mapped_df, lift_from_model, lift_to_model, args.verbose)

    
    # Do String based Property mapping if requested
    if configs['string_match_mapping']:
        if args.verbose:
            print("Starting Property string-based mapping")
        #mapped_df = doStringPropertyMapping(lift_from_model, lift_to_model, mapped_df, args.verbose)
        #mapped_df = doStringPropertyMapping(lift_from_model, lift_to_model, mapped_df, False)
        mapped_df = doStringPropertyMapping2(lift_from_dict_df, lift_to_dict_df, mapped_df, lift_from_model, lift_to_model, args.verbose)
    
    # Done with property mapping, print the file
    if args.verbose:
        print("Print CDE/String Mapping File")
    mapped_df.to_csv(configs['mapping_file'], sep="\t", index=False)
        
    #
    #    Value mapping
    #   
    if configs['value_map_file'] != 'None':
        #print(f"value_map_file is: {configs['value_map_file']}")
        # Add PVs and concept codes to the existing from and to_from_dict_df
        lift_from_dict_df = conceptCodifier(lift_from_dict_df)
        lift_to_dict_df = conceptCodifier(lift_to_dict_df)
        #print(lift_from_dict_df)
        if args.verbose:
            print("Starting Value mapping")
        #mapped_pv_df = doConceptCodeValueMapping(lift_from_cdeinfo, lift_to_cdeinfo, lift_from_model, lift_to_model, lift_from_dict, lift_to_dict, mapped_pv_df, args.verbose)
        #mapped_pv_df = doConceptCodeValueMapping(lift_from_cdeinfo, lift_to_cdeinfo, lift_from_model, lift_to_model, lift_from_dict, lift_to_dict, mapped_pv_df, False)
        mapped_pv_df = doConceptCodeValueMapping2(lift_from_dict_df, lift_from_dict_df, lift_from_model, lift_to_model, mapped_pv_df, args.verbose)
        # Do synonym if requested
        if configs['synonym_mapping']:
            if args.verbose:
                print("Starting synonym mapping")
            #mapped_pv_df = doSynonymValueMapping(lift_from_model, lift_to_model, mapped_pv_df, args.verbose)
            #mapped_pv_df = doSynonymValueMapping(lift_from_model, lift_to_model, mapped_pv_df, False)
        
            
    if configs['value_map_file'] != 'None':
        if args.verbose:
            print("Printing Value Mapping file")
        mapped_pv_df.to_csv(configs['value_map_file'], sep="\t", index=False)
   

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument("-v", "--verbose", action='store_true', help="Verbose Output")

    args = parser.parse_args()

    main(args)