# MDFModelMapper
A Collection of scripts that map between models and (hopefully) create a useful liftover file.  The input models must be in MDF format.  All output files are tab-separated text files.

## MDFModelMapper
**Usage**: python MDFModelMapper.py -c <configfile> -v <verbose> -h <help>\
-c: (Required) A valid path to the configuration file\ 
-v: (Optional) Verbosity level.  Verbosity can be increadsed by using additional v's (-vv, -vvv)\
-h: (Optional) Display help information\

Takes a YAML config file as input (see *mapping_configs.yml*) and can map with several different options

- CDE Mapping - Compares CDE IDs between model properties
- String mapping - Copmares string names between model properties that have not been mapped by CDE ID
- Value Mapping - Compares permissible values by the concept code from NCI Thesaurus
- Synonym Mapping - Uses permissible value synonyms to map between PVs

Output is a tab-delimited file

## MDFMappingAnalysis
**Usage**: python MDFMappingAnalysis.py -c <configfile> -v <verbose> -h <help>\
-c: (Required) A valid path to the configuration file\ 
-v: (Optional) Verbosity level.  Verbosity can be increadsed by using additional v's (-vv, -vvv)\
-h: (Optional) Display help information\ 

Uses the output from *MDFModelMapper* and the same YAML configuration file as *MDFModelMapper* and looks for mapping mismatches:
- Node Mismatch:  The property maps to a different node.  This can be due to legitimate model changes or to use of the same CDE/property name in multiple nodes.
- Property Name Mismatch - This is the result of a CDE ID match where the property name differs.  There can be legitimate reasons for this but it frequently indicates some sort of mapping error
- CDE ID mismatch: This is the result of the properties matching on the string, but either having different CDEs or only one having a CDE.


## MismatchDematchifyer
**Usage**: python MismatchDematchifyer.py -c <configfile> -v <verbose> -h <help>\
-c: (Required) A valid path to the configuration file\ 
-v: (Optional) Verbosity level.  Verbosity can be increadsed by using additional v's (-vv, -vvv)\
-h: (Optional) Display help information\ 

Takes a YAML config file (see *mismatchremoval_config.yml*) adn will use the outputs of *MDFModelMapper* and *MDFMappingAnalysis* to remove incorrect mismatchs from the mapping file.  It is **STRONGLY** recommended that the output from *MDFMappingAnalysis* is edited first to remove any lines that are actually correct mappings.  Each remaning line in the output from *MDFMappingAnalysis* will be removed from the output of *MDFModelMapper*.



# YAML configurations
## MDFModelMapper, MDFMappingAnalysis
- *lift_from_model_files*: A list of MDF files (can be a list of GitHub RAW URLs) that represnt the model that will be the source for mapping.
- *lift_from_to_files*: A list of MDF files (can be a list of GitHub RAW URLs) that represnt the model that will be the target for mapping.
- *cde_mapping*: A Boolean value.  True enables CDE ID property mapping, False prevents CDE ID property mapping.
- *string_match_mapping*: A Boolean value.  True enables string-based property mapping, False prevents string-based property mapping.
- *value_mapping*: A Boolean value.  True enables permissible value mapping by concept code, False prevents PV mapping.
- *synonym_mapping*: A Boolean value.  True enables permissible value mapping by synonyms, False prevents synonym PV mapping.
- *mapping_report*: Boolean.  If True, a mapping report will be saved.
- *unmapped_report*: Boolean.  If True, the unmapped property report will be saved.
- *autoname*: Boolean.  If True, files will be automaticallly named using source and target model names and version.
- *savepath*: Stirng.  A valid path where all output files will be saved.
- *mapping_file*:  String.  This is the file name for the mapping report.  This is not used of autoname is True.
- *value_map_file*: String.  This is the filename for the value mapping report.  This is not used if autoname is True.
- *mapping_mismatch_file*: String.  This is the filename for the output from MDFMappingAnalysis.  This is not used of autoname is True

## MismatchDematchifyer
- *filepath*: String.  A valid path where files will be found and written.
- *mappingfile*: String.  The output file from *MDFModelMapper*.
- *mismatchfile*: String.  The output file from *MDFMappingAnalysis* (edit to remove any lines that are correct mappings.)
- *savefile* String.  The file where the cleaned mapping informaiton will be saved.