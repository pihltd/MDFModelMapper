import bento_mdf
import pandas as pd
import argparse
import yaml

def readYAML(yamlfile):
    with open(yamlfile) as f:
        yamljson = yaml.load(f, Loader=yaml.FullLoader)
    return yamljson


def CDEDictionary(props):
    # Create a dictionary with the CDEID as the key and the property name as the value
    id_dict = {}
    for prop in props:
        if props[prop].concept is not None:
            workingterms = props[prop].concept.terms.values()
            for workingtermobj in workingterms:
                workingterm = workingtermobj.get_attr_dict()
                #CDEID:{property name: node name}
                id_dict[workingterm['origin_id']] = {prop[1]:prop[0]}
    return id_dict
                


def main(args):
    configs = readYAML(args.configfile)
    
    mapped_df_headers = ['lift_from_node',
                         'lift_from_prop',
                         'lift_from_cdeID',
                         'lift_from_model',
                         'lift_from_version',
                         'lift_to_node',
                         'lift_to_prop',
                         'lift_to_cdeID',
                         'lift_to_model',
                         'lift_to_version']
    mapped_df = pd.DataFrame(columns=mapped_df_headers)
    
    lift_from_model = bento_mdf.MDF(*configs['lift_from_model_files'])
    lift_to_model = bento_mdf.MDF(*configs['lift_to_model_files'])
    
    lift_from_dict = CDEDictionary(lift_from_model.model.props)
    lift_to_dict = CDEDictionary(lift_to_model.model.props)
    
    for cdeid, propinfo in lift_from_dict.items():
        if cdeid in lift_to_dict:
            mapped_df.loc[len(mapped_df)] = {'lift_from_node': list(propinfo.values())[0],
                                             'lift_from_prop': list(propinfo.keys())[0],
                                             'lift_from_cdeID': cdeid,
                                             'lift_from_model': lift_from_model.model.handle,
                                             'lift_from_version': lift_from_model.model.version,
                                             'lift_to_node': list(lift_to_dict[cdeid].values())[0],
                                             'lift_to_prop': list(lift_to_dict[cdeid].keys())[0],
                                             'lift_to_cdeID': cdeid,
                                             'lift_to_model': lift_to_model.model.handle,
                                             'lift_to_version': lift_to_model.model.version}
        else:
            mapped_df.loc[len(mapped_df)] = {'lift_from_node': list(lift_from_dict[cdeid].values())[0],
                                             'lift_from_prop': list(lift_from_dict[cdeid].keys())[0],
                                             'lift_from_cdeID': cdeid,
                                             'lift_from_model': lift_from_model.model.handle,
                                             'lift_from_version': lift_from_model.model.version,
                                             'lift_to_node': 'None',
                                             'lift_to_prop': 'None',
                                             'lift_to_cdeID': 'None',
                                             'lift_to_model': 'None',
                                             'lift_to_version': 'None'}
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
                                             'lift_to_version': lift_to_model.model.version}
    
    
    mapped_df.to_csv(configs['mapping_file'], sep="\t")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument("-v", "--verbose", help="Verbose Output")

    args = parser.parse_args()

    main(args)