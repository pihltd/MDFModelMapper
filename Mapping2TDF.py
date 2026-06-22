import pandas as pd
from crdclib import crdclib



inputfile = r'C:\Users\pihltd\Documents\VMShare\ModelMapping\CRDCSubmission2GC\CRDC_Submission_0.1-CDS_v12.0.0_DEMISMATCHED_ANNOTATED.tsv'
tdf = {}

map_df = pd.read_csv(inputfile, sep="\t")
temp = {}
for index, row in map_df.iterrows():
    inputinfo = {'Model': row['lift_from_model'], 'Version': row['lift_from_version'], 'Node': row['lift_from_node'], 'Props': row['lift_from_prop']}
    outputinfo = {'Model': row['lift_to_model'], 'Version': row['lift_to_version'], 'Node': row['lift_to_node'], 'Props': row['lift_to_prop']}
    transformname = f"{row['lift_from_prop']}_to_{row['lift_to_prop']}"
    temp[transformname] = {'Inputs': [inputinfo], 'Outputs': [outputinfo]}
    temp[transformname]['Steps'] = [{'Entrypoint': 'copy_this_thing', 'Package': 'tdp_python_mess@0.0.1', 'Params':{'delimiter': " "}}]
    
tdf['Transforms'] = temp

outputfile = r'C:\Users\pihltd\Documents\VMShare\ModelMapping\CRDCSubmission2GC\CRDC_Submission_0.1-CDS_v12.0.0_TDF.yml'

crdclib.writeYAML(filename=outputfile, jsonobj=tdf)