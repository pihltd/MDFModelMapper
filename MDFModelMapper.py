import bento_mdf
import pandas as pd
import numpy as np
import argparse
import yaml
import requests


def stsPVQuery(id, version, model=False, verbose=0):
    if verbose >= 2:
        print(f"stsPVQuery for ID: {id} and version {version}")
    
    if model:
        url = f"https://sts-dev.cancer.gov/v1/terms/model-pvs/{id}/{version}/pvs"
    else:
        url = f"https://sts.cancer.gov/v1/terms/cde-pvs/{id}/{version}/pvs"
    headers = {'accept': 'application/json'}
    
    if verbose >= 2:
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



def readYAML(yamlfile):
    with open(yamlfile) as f:
        yamljson = yaml.load(f, Loader=yaml.FullLoader)
    return yamljson

def getCDEID(props, prop, verbose= 0):
    if verbose >= 2:
        print(f"getCDEID for Props: {props} and Prop: {prop}")
    if props[prop].concept is not None:
        workingterms = props[prop].concept.terms.values()
        for workingtermobj in workingterms:
            workingterm = workingtermobj.get_attr_dict()
            if verbose >= 3:
                print(f"Starting object: {workingtermobj}")
                print(f"Prop: {prop}\t\tWorkingterm:\n{workingterm}")
                print(f"geCDEID InfoL  Prop: {prop[1]}\tNode: {prop[0]}\tCDE: {workingterm['origin_id']}\t Ver: {workingterm['origin_version']}")
            return workingterm['origin_id'], workingterm['origin_version']
    else:
        return None, None

def CDEDataFrame(props, verbose = 0):
    if verbose >= 2:
        print(f"CDEDataFrame for Props {props}")
    columns = ["cde_id", "cde_version", "property_name", "node_name"]
    final_df = pd.DataFrame(columns=columns)
    for prop in props:
        if props[prop].concept is not None:
            cdeid, cdeversion = getCDEID(props, prop)
            final_df.loc[len(final_df)] = {"cde_id": cdeid, "cde_version":cdeversion, "property_name": prop[1], "node_name": prop[0]}
            cdeid = np.nan
            cdeversion = np.nan
        else:
            # This covers situations where there is no CDE.  Useful if string matching is done.
            final_df.loc[len(final_df)] = {"cde_id": np.nan, "cde_version":np.nan, "property_name": prop[1], "node_name": prop[0]}
    return final_df

                

def PVDictionary(cdeid, cdeversion,  verbose=0):
    #Returns a dictionary of {concept code: PV term}.
    if verbose >= 2:
        print(f"PVDictionary for CDE ID {cdeid} and version {cdeversion}")
    final = {}
    #synonyms = {}
    cdejson = stsPVQuery(cdeid, cdeversion, False, verbose)
    if verbose >= 3:
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


def SynonymDictionary(modelhandle, modelversion, verbose=0):
    # Returns a dicitonary of {concept code: [synonyms]} 
    # The standard is to put a "v" in front of the model version.  This nukes that because the STS API doesn't like that
    modelversion = modelversion.replace("v", "")
    if verbose >= 2:
        print(f"SynonymDictionary for {modelhandle} version {modelversion}")
    pvjson = stsPVQuery(modelhandle, modelversion, True, verbose)
    if verbose >= 3:
        print(pvjson)
    final = {}
    
    if pvjson is not None:
        for cdelist in pvjson['permissibleValues']:
            for cde in cdelist:
                if verbose >= 3:
                    print(cde)
                if cde['ncit_concept_code'] is not None:
                    if len(cde['synonyms']) > 0:
                        final[cde['ncit_concept_code']] = cde['synonyms']
    else:
        pvjson = None
    return final



def doCDEPropertyMapping(from_df, to_df, mapped_df, lift_from_model, lift_to_model,verbose=0):
    if verbose >= 2:
        print("doCDEPropertyMapping")
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
            


def doStringPropertyMapping(from_df, to_df, mapped_df, lift_from_model, lift_to_model, verbose=0):
    # Map properties by sting matching
    if verbose >= 2:
        print("Running doStringPropertyMapping")
    for index, row in from_df.iterrows():
        if row['property_name'] not in mapped_df['lift_from_prop'].unique():
            query = row['property_name']
            to_row_df = to_df.query('property_name == @query')
            for index, to_row in to_row_df.iterrows():
                if verbose >= 2:
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
                


def conceptCodifier(df, verbose=0):
    if verbose >= 2:
        print("Running conceptCodifier")
    if verbose >= 3:
        print(f"Input Dataframe:\n{df.head()}")
    # {concept code: PV term}
    columns = ["cde_id", "cde_version", "property_name", "node_name", "permissible_value", "concept_code"]
    new_df = pd.DataFrame(columns=columns)
    for index, row in df.iterrows():
        pvstuff = PVDictionary(row['cde_id'], row['cde_version'], verbose)
        if verbose >=3:
            print(pvstuff)
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
        

def doConceptCodeValueMapping(from_df, to_df, lift_from_model, lift_to_model, mapped_pv_df, verbose = 0):
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
    # NOTE: This needs a little rethinking.  Matching on JUST concept codes makes some weird mappings because of how widely used some concept codes are.
    # Maybe only allow if the CDE matches?  If the goal is transformation, that might make sense.h
    
    if verbose >= 2:
        print("Running doConceptCodeValueMapping")
    for index, row in from_df.iterrows():
        if row['concept_code'] in to_df['concept_code'].unique():
            cc = row['concept_code']
            temp_df = to_df.query('concept_code ==@cc')
            for to_index, to_row in temp_df.iterrows():
                # Filter on CDE ID because of how wide ranging some concept codes are
                if row['cde_id'] == to_row['cde_id']:
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



def doSynonymValueMapping(from_df, to_df, mapped_pv_df, lift_from_model, lift_to_model, verbose = 0):
    if verbose >= 2:
        print("Running doSynonymValueMapping")
    lift_to_synonyms = SynonymDictionary(lift_to_model.model.handle, lift_to_model.model.version, verbose)
    if verbose >=3:
        print(lift_to_synonyms)
    # {concept code: [synonyms]} 
    for index, row in from_df.iterrows():
        # Check to see if the permissible value has already been mapped by concept code
        #if row['concept_code'] not in mapped_pv_df['lift_from_conceptCode'].unique():
        # ALTERNATE:  Has it been mapped by string?
        if row['permissible_value'] not in mapped_pv_df['lift_from_pv'].unique():
            if verbose >= 3:
                print(f"PV {row['permissible_value']} not mapped by Concept Code")
            # Check to see if the concept code is represented in the synonyms
            # NOTE:  I'm not sure I should be doing this.
            #if row['concept_code'] in lift_to_synonyms.keys():
                # Loop through the synonyms to see if there is a match
                for synlist in lift_to_synonyms.values():
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

def propertyReport(from_df, to_df, mapping_df, from_model, to_model, filename, verbose=0):
    # Print a basic report to a file
    if verbose >= 2:
        print(f"Saving report to {filename}")
    total_from_props = len(list(from_df['property_name'].unique()))
    total_to_props = len(list(to_df['property_name'].unique()))
    mapped_from_props = len(mapping_df['lift_from_prop'].unique())
    mapped_to_props = len(mapping_df['lift_to_prop'].unique())
    with open(filename, "w") as f:
        f.write(f"From {from_model.model.handle} version {from_model.model.version} mapping To {to_model.model.handle} version {to_model.model.version}\n")
        f.write(f"Total number of props in {from_model.model.handle} {from_model.model.version}:\t{total_from_props}\n")
        f.write(f"Total number of mapped props in {from_model.model.handle}  {from_model.model.version}:\t {mapped_from_props}\n")
        f.write(f"Total number of props in {to_model.model.handle}  {to_model.model.version}:\t{total_to_props}\n")
        f.write(f"Total number of mapped props in {to_model.model.handle}  {to_model.model.version}:\t {mapped_to_props}\n")
        f.write(f"Starting {from_model.model.handle} {from_model.model.version} Property List:\n{list(from_df['property_name'].unique())}\n")
        f.write(f"Mapped {from_model.model.handle} {from_model.model.version} Properties:\n{list(mapping_df['lift_from_prop'].unique())}\n")
    f.close()
    
def dropDFRow(df, df_field, mappedlist, mdf):
    for index, row in df.iterrows():
        if row[df_field] in mappedlist:
           # df.drop(index, inplace=True)
           df=df.drop(index)
        df['model'] = mdf.model.handle
        df['version'] = mdf.model.version
    return df
        
        
        
def unMappedReport(from_df, to_df, mapping_df, from_model, to_model,filename,df_type, verbose=0):
    if df_type == 'props':
        map_from_field = 'lift_from_prop'
        map_to_field = 'lift_to_prop'
        df_field = 'property_name'
    else: 
        map_from_field = 'lift_from_pv'
        map_to_field = 'lift_to_pv'
        df_field = 'permissible_value'        
    if verbose >=2:
        print(f"Saving unmapped report {filename}")
    from_mapped_list = mapping_df[map_from_field].unique()
    to_mapped_list = mapping_df[map_to_field].unique()
    from_unmapped_df = dropDFRow(from_df, df_field, from_mapped_list, from_model)
    to_unmapped_df = dropDFRow(to_df, df_field, to_mapped_list, to_model)
    from_unmapped_df.to_csv(filename, sep="\t", index=False)
    to_unmapped_df.to_csv(filename, mode='a', sep="\t", index=False, header=False)


def main(args):
    
    # Mapping order
    # 1 - Property-Property by CDE match
    # 2 - Property-Property by String match
    # 3 - Value-Value by concept code match
    # 4 - Value-Value by synonym
    
    if args.verbose >= 1:
        print("Doing setup work")
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
      
    lift_from_df = CDEDataFrame(lift_from_model.model.props)
    lift_to_df = CDEDataFrame(lift_to_model.model.props)
    
    if args.verbose >= 1:
        print(f"From {lift_from_model.model.handle} version {lift_from_model.model.version} To {lift_to_model.model.handle} version {lift_to_model.model.version}")
    
    #
    #  Property Mapping
    #
    
    
    if args.verbose >= 1:
        print("Starting Property CDE based mapping")
    
    # Do CDE based Property mapping if requested
    if configs['cde_mapping']:
        if args.verbose >= 1:
            print("Starting CDE Mapping")
        mapped_df = doCDEPropertyMapping(lift_from_df, lift_to_df, mapped_df, lift_from_model, lift_to_model, args.verbose)
    
    # Do String based Property mapping if requested
    if configs['string_match_mapping']:
        if args.verbose >= 1:
            print("Starting Property string-based mapping")
        mapped_df = doStringPropertyMapping(lift_from_df, lift_to_df, mapped_df, lift_from_model, lift_to_model, args.verbose)
        if args.verbose >=2:
            print(f"String mapping lift_from_df:\n{lift_from_df.head()}")
    
    # Done with property mapping, print the file
    if args.verbose >= 1:
        print("Print CDE/String Mapping File")
    if configs['autoname']:
        savefile = f"{lift_from_model.model.handle}_{lift_from_model.model.version}-{lift_to_model.model.handle}_{lift_to_model.model.version}.tsv"
        mapped_df.to_csv(configs['savepath']+savefile, sep="\t", index=False)
    else:
        mapped_df.to_csv(configs['mapping_file'], sep="\t", index=False)
    if configs['mapping_report']:
        savefile = f"{lift_from_model.model.handle}_{lift_from_model.model.version}-{lift_to_model.model.handle}_{lift_to_model.model.version}--REPORT.txt"
        if args.verbose >=1:
            print(f"Saving mapping report {savefile}")
        propertyReport(lift_from_df, lift_to_df, mapped_df, lift_from_model, lift_to_model,configs['savepath']+savefile, args.verbose )
        if args.verbose >=2:
            print(f"Property Report lift_from_df:\n{lift_from_df.head()}")
    if configs['unmapped_report']:
        savefile = f"{lift_from_model.model.handle}_{lift_from_model.model.version}-{lift_to_model.model.handle}_{lift_to_model.model.version}--UNMAPPED_REPORT.tsv"
        if args.verbose >=1:
            print(f"Saving umapped report {savefile}")
        unMappedReport(lift_from_df, lift_to_df, mapped_df,lift_from_model, lift_to_model, configs['savepath']+savefile, 'props', args.verbose)          
        
        
    #
    #    Value mapping
    #   
    if configs['value_mapping']:
        # Add PVs and concept codes to the existing from and to_from_dict_df
        if args.verbose >= 1:
            print("Adding concept codes to dataframe")
        lift_from_df = conceptCodifier(lift_from_df, args.verbose)
        lift_to_df = conceptCodifier(lift_to_df, args.verbose)
        if args.verbose >= 1:
            print("Starting Value mapping")
        mapped_pv_df = doConceptCodeValueMapping(lift_from_df, lift_to_df, lift_from_model, lift_to_model, mapped_pv_df, args.verbose)
        # Do synonym if requested
        if configs['synonym_mapping']:
            if args.verbose >= 1:
                print("Starting synonym mapping")
            mapped_pv_df = doSynonymValueMapping(lift_from_df, lift_to_df, mapped_pv_df, lift_from_model, lift_to_model, args.verbose)
            
            
    if configs['value_mapping']:
        if args.verbose >= 1:
            print("Printing Value Mapping file")
        if configs['autoname']:
            savefile = f"{lift_from_model.model.handle}_{lift_from_model.model.version}-{lift_to_model.model.handle}_{lift_to_model.model.version}_ValueMapping.tsv"
            mapped_pv_df.to_csv(configs['savepath']+savefile, sep="\t", index=False)
        else:
            mapped_pv_df.to_csv(configs['value_map_file'], sep="\t", index=False)
        if configs['unmapped_report']:
            if args.verbose >=1:
                print("Printing Unmapped Report file")
            savefile = f"{lift_from_model.model.handle}_{lift_from_model.model.version}-{lift_to_model.model.handle}_{lift_to_model.model.version}--UNMAPPED_PV_REPORT.tsv"
            unMappedReport(lift_from_df, lift_to_df, mapped_pv_df,lift_from_model, lift_to_model, configs['savepath']+savefile, 'values', args.verbose)
   

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument('-v', '--verbose', action='count', default=0, help=("Verbosity: -v main section -vv subroutine messages -vvv data returned shown"))

    args = parser.parse_args()

    main(args)