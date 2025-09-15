import bento_mdf
import pandas as pd
import argparse
import yaml
import requests

def stsPVQuery(cdeid, cdeversion):
    
    #url = f"https://sts-dev.ctos-data-team.org/v1/terms/cde-pvs/{cdeid}/{cdeversion}/pvs"
    url = f"https://sts.cancer.gov/v1/terms/cde-pvs/{cdeid}/{cdeversion}/pvs"
    headers = {'accept': 'application/json'}
    
    #print(f"URL: {url}")
    try:
        result = requests.get(url = url, headers = headers)

        if result.status_code == 200:
            return result.json()
        else:
            print(f"Error: {result.status_code}")
            return result.content
    except requests.exceptions.HTTPError as e:
        return(f"HTTP Error: {e}")

def readYAML(yamlfile):
    with open(yamlfile) as f:
        yamljson = yaml.load(f, Loader=yaml.FullLoader)
    return yamljson

def getCDEID(props, prop):
    workingterms = props[prop].concept.terms.values()
    for workingtermobj in workingterms:
        workingterm = workingtermobj.get_attr_dict()
        return workingterm['origin_id'], workingterm['origin_version']

def CDEDictionary(props):
    # id_dict is a dictionary with the CDEID as the key and a dicitonary of property name: node name as the value
    #CDEID:{property name: node name}
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
    return id_dict

def StringDictionary(props):
    # Creates a dictionary with property names as key and a dictionary of node:CDEID as the value
    # PropName:{NodeName:CDEID}
    prop_dict = {}
    for prop in props:
        if props[prop].concept is not None:
            cdeid = getCDEID(props, prop)
        else:
            cdeid = 'None'
        prop_dict[prop[1]] = {prop[0]:cdeid}
    return prop_dict
                
def CDEInfo(mdf):
    # Returns a dictionary of cdeid:cde veraion
    props = mdf.model.props
    final = {}
    for prop in props:
        if props[prop].concept is not None:
            workingterms = props[prop].concept.terms.values()
            for workingtermobj in workingterms:
                workingterm = workingtermobj.get_attr_dict()
                final[workingterm['origin_id']] = workingterm['origin_version']
    return final
                

def PVDictionary(cdeid, cdeversion):
    #Returns a dictionary of {concept code: PV term}
    final = {}
    cdejson = stsPVQuery(cdeid, cdeversion)
    # If multiple models have different names for a CDE it might come back as a list
    if type(cdejson['CDECode']) is list:
        if len(cdejson['permissibleValues'][0]) > 0:
            for pv in cdejson['permissibleValues'][0]:
                final['ncit_concept_code'] = pv['value']
        else:
            final = None
    elif len(cdejson['permissibleValues']) > 0:
        #print(cdejson)
        for pv in cdejson['permissibleValues']:
            final[pv['ncit_concept_code']] = pv['value']
    else:
        final = None
    return final
    


def main(args):
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
                         'lift_t0_conceptCode',
                         'lift_to_model',
                         'lift_to_version',
                         'mapping_type']
    
    mapped_df = pd.DataFrame(columns=mapped_df_headers)
    mapped_pv_df = pd.DataFrame(columns=pv_df_mapping_headers)
    
    lift_from_model = bento_mdf.MDF(*configs['lift_from_model_files'])
    lift_to_model = bento_mdf.MDF(*configs['lift_to_model_files'])
    
    lift_from_dict = CDEDictionary(lift_from_model.model.props)
    lift_to_dict = CDEDictionary(lift_to_model.model.props)
    
    lift_from_cdeinfo = CDEInfo(lift_from_model)
    lift_to_cdeinfo = CDEInfo(lift_to_model)
    
    
    if args.verbose:
        print("Starting CDE based mapping")
    
    for cdeid, propinfolist in lift_from_dict.items():
        #NOTE:  propinfolist is a list of dictionary [{CDEID:{property name: node name}}]
        if cdeid in lift_to_dict:
            for propinfo in propinfolist:
                if len(lift_to_dict[cdeid]) == 1:
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
                # Only do value mapping on mapped CDEs if requested
                # TODO:  Figure out why concepts without a concept code are being mapped anyways
                if configs['value_map_file'] is not None:
                    pv_from_codes = PVDictionary(cdeid, lift_from_cdeinfo[cdeid])
                    pv_to_code = PVDictionary(cdeid, lift_to_cdeinfo[cdeid])
                    if pv_from_codes is not None:
                        for conceptcode, pvtext in pv_from_codes.items():
                            if conceptcode in pv_to_code:
                                if 'C' not in conceptcode:
                                    print(f"From Concept Code: {conceptcode}\tText: {pvtext}")
                                    print(f"To Concept Record: {pv_to_code[conceptcode]}")
                                mapped_pv_df.loc[len(mapped_pv_df)] = {'lift_from_node': list(propinfo.values())[0],
                                'lift_from_prop': list(propinfo.keys())[0],
                                'lift_from_cdeID': cdeid,
                                'lift_from_cdeVersion':lift_from_cdeinfo[cdeid] ,
                                'lift_from_pv': pvtext,
                                'lift_from_conceptCode': conceptcode,
                                'lift_from_model': lift_from_model.model.handle,
                                'lift_from_version': lift_from_model.model.version,
                                'lift_to_node': list(lift_to_dict[cdeid][0].values())[0],
                                'lift_to_prop': list(lift_to_dict[cdeid][0].keys())[0],
                                'lift_to_cdeID': cdeid,
                                'lift_to_cdeVersion': lift_to_cdeinfo[cdeid],
                                'lift_to_pv': pv_to_code[conceptcode],
                                'lift_t0_conceptCode': conceptcode,
                                'lift_to_model': lift_to_model.handle,
                                'lift_to_version': lift_to_model.version,
                                'mapping_type': 'Concept Code Mapping'}
                            else:
                                mapped_pv_df.loc[len(mapped_pv_df)] = {'lift_from_node': list(propinfo.values())[0],
                                'lift_from_prop': list(propinfo.keys())[0],
                                'lift_from_cdeID': cdeid,
                                'lift_from_cdeVersion':lift_from_cdeinfo[cdeid] ,
                                'lift_from_pv': pvtext,
                                'lift_from_conceptCode': conceptcode,
                                'lift_from_model': lift_from_model.model.handle,
                                'lift_from_version': lift_from_model.model.version,
                                'lift_to_node': 'None',
                                'lift_to_prop': 'None',
                                'lift_to_cdeID': 'None',
                                'lift_to_cdeVersion': 'None',
                                'lift_to_pv':'None',
                                'lift_t0_conceptCode': 'None',
                                'lift_to_model':'None',
                                'lift_to_version': 'None',
                                'mapping_type': 'None'}
        else:
            for propinfo in propinfolist:
                mapped_df.loc[len(mapped_df)] = {'lift_from_node': list(propinfo.values())[0],
                                                'lift_from_prop': list(propinfo.keys())[0],
                                                'lift_from_cdeID': cdeid,
                                                'lift_from_cdeVersion': lift_from_cdeinfo[cdeid],
                                                'lift_from_model': lift_from_model.model.handle,
                                                'lift_from_version': lift_from_model.model.version,
                                                'lift_to_node': 'None',
                                                'lift_to_prop': 'None',
                                                'lift_to_cdeID': 'None',
                                                'lift_to_cdeVersion': 'None',
                                                'lift_to_model': 'None',
                                                'lift_to_version': 'None',
                                                'mapping_type': 'None'}
            
            
    # If requested do string matching 
    if configs['string_match_mapping']:
        if args.verbose:
            print("Starting string-based matching")
        if configs['cde_only_mapping'] is not None:
            # Print out the results of the CDE based mapping just in case
            mapped_df.to_csv(configs['cde_only_mapping'], sep="\t", index=False)
        
        lift_from_string = StringDictionary(lift_from_model.model.props)
        lift_to_string = StringDictionary(lift_to_model.model.props)
        
        for propname, nodeinfo in lift_from_string.items():
            # Only map if not already mapped by CDE
            if propname not in mapped_df['lift_from_prop'].unique():
                if propname in lift_to_string:
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
        
    # Not sure why I included this, it's really only of academic interest        
    if configs['reverse_mapping']:
        for cdeid, propinfo in lift_to_dict.items():
            if cdeid not in lift_from_dict:
                mapped_df.loc[len(mapped_df)] = {'lift_from_node': 'None',
                                                'lift_from_prop': 'None',
                                                'lift_from_cdeID': 'None',
                                                'lift_from_model':  'None',
                                                'lift_from_version':  'None',
                                                'lift_to_node': list(propinfo.values())[0],
                                                'lift_to_prop': list(propinfo.keys())[0],
                                                'lift_to_cdeID': cdeid,
                                                'lift_to_model': lift_to_model.model.handle,
                                                'lift_to_version': lift_to_model.model.version,
                                                'mapping_type': 'None'}
    
    
    mapped_df.to_csv(configs['mapping_file'], sep="\t", index=False)
    
    if configs['value_map_file'] is not None:
        mapped_pv_df.to_csv(configs['value_map_file'], sep="\t", index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument("-v", "--verbose", help="Verbose Output")

    args = parser.parse_args()

    main(args)