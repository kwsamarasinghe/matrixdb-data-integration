import logging
import math

import pandas as pd
import requests
import re

# Function to make an HTTP call and parse the response to JSON
complex_portal_api = 'https://www.ebi.ac.uk/intact/complex-ws/complex/'
def call_complex_portal_api(cpx_id):
    try:
        response = requests.get(f'{complex_portal_api}{cpx_id}')
        response.raise_for_status()
        data = response.json()
        return data

    except requests.exceptions.RequestException as err:
        print(f'Error fetching data: {err}')


def get_complex_portal_data(cpx):
    complex_portal_data = call_complex_portal_api(cpx)

    if complex_portal_data is None:
        return

    # Functions
    function = None
    if "functions" in complex_portal_data:
        function = complex_portal_data["functions"][0]

    go_terms = list()
    for go in list(filter(lambda xref: xref["database"] == 'gene ontology', complex_portal_data["crossReferences"])):
        go_terms.append(go["identifier"])

    pdbs = list()
    for pdb in list(filter(lambda xref: xref["database"] == 'wwpdb', complex_portal_data["crossReferences"])):
        pdbs.append(pdb["identifier"])

    reactome_xrefs = list(filter(lambda xref: xref["database"] == 'reactome', complex_portal_data["crossReferences"]))
    reactome_xref = None
    if len(reactome_xrefs) > 0:
        reactome_xref = list({'id': rf['identifier']} for rf in reactome_xrefs)

    complex_portal_annotations = dict()
    if function is not None:
        complex_portal_annotations["function"] = function

    if len(go_terms) > 0:
        complex_portal_annotations["go"] = go_terms

    if len(pdbs) > 0:
        complex_portal_annotations["pdbs"] = pdbs

    if reactome_xref is not None:
        complex_portal_annotations["xrefs"] = dict()
        complex_portal_annotations["xrefs"]["reactome"] = reactome_xref

    if "synonyms" in complex_portal_data:
        complex_portal_annotations["other_names"] = complex_portal_data["synonyms"]

    if "properties" in complex_portal_data and len(complex_portal_data["properties"]):
        complex_portal_annotations["description"] = complex_portal_data["properties"][0]

    return complex_portal_annotations


def load_core_biomolecules(config, source, target):
    """
    Loads multimers either from the database, if exists
    complementarity, if does not exist in the database, create a new one and get additional data from complex portal
    """
    core_biomols = list()
    # Read and get the list of multimers from the custom biomolecule file
    df = pd.read_csv(config["dependencies"]["core"]["raw"], dtype='str', sep='\t')
    multimers = df[df['MatrixDB identifier'].str.contains("MULT_", case=False, na=False)]
    for row_id, multimer_row in multimers.iterrows():
        existing_multimer = source["biomolecules"].find_one({
            'id': multimer_row[0]
        })

        complex_portal_data = None
        if existing_multimer is not None:
            del existing_multimer["_id"]
            if "keywords" in existing_multimer["annotations"]:
                del existing_multimer["annotations"]["keywords"]

            # Need to fetch complex portal data
            if "xrefs" not in existing_multimer:
                core_biomols.append(existing_multimer)
                continue

            if "complex_portal" not in existing_multimer["xrefs"]:
                core_biomols.append(existing_multimer)
                continue

            cpx = existing_multimer["xrefs"]["complex_portal"]
            complex_portal_data = get_complex_portal_data(cpx)
            new_multimer = existing_multimer
        else:

            complex_portal_id = multimer_row[4]
            if complex_portal_id is not None and type(complex_portal_id) != float:
                complex_portal_data = get_complex_portal_data(complex_portal_id)

            # Create a biomolecule object
            id = multimer_row[0]
            common_name = multimer_row[1]
            other_name = multimer_row[2]
            if type(other_name) is not list:
                other_name = [other_name]
            species = re.compile(r'[0-9]+').search(multimer_row[3]).group(0)
            complex_portal_id = multimer_row[4]
            comments = multimer_row[5]
            if comments == '':
                comments = complex_portal_data["description"]
            other_info = multimer_row[6]

            new_multimer = {
                "id": id,
                "type": 'multimer',
                "species": species,
                "names": {
                    "name": common_name
                },
                "xrefs": {
                    "complex_portal": complex_portal_id
                },
                "_meta": {
                    "from_version": "4.0"
                }
            }

            if comments is not None:
                new_multimer["description"] = comments

            if other_info is not None:
                new_multimer["comment"] = other_info

            if other_name is not None:
                new_multimer["names"]["common_name"] = other_name,

        if complex_portal_data is not None:
            new_multimer["names"]["other_names"] = complex_portal_data["other_names"]

            annotations = dict()
            if "function" in complex_portal_data:
                annotations["function"] = complex_portal_data["function"]

            if "pdbs" in complex_portal_data:
                if "molecular_details" not in annotations:
                    new_multimer["molecular_details"] = dict()
                new_multimer["molecular_details"]["pdb"] = complex_portal_data["pdbs"]
                if "pdb" in new_multimer["xrefs"]:
                    del new_multimer["xrefs"]["pdb"]

            if "go" in complex_portal_data:
                annotations["go"] = complex_portal_data["go"]

            if "xrefs" in complex_portal_data and "reactome" in complex_portal_data["xrefs"]:
                new_multimer["xrefs"]["reactome"] = complex_portal_data["xrefs"]["reactome"]

            new_multimer["annotations"] = annotations
            if "_meta" in new_multimer:
                new_multimer["_meta"]["core"] = True
        else:
            logging.warning({
                "message": f'No complex portal data for {new_multimer["id"]}'
            })

        core_biomols.append(new_multimer)

    for b in list(source["biomolecules"].find({
        'type': {
            '$ne': 'multimer'
        }
    })):
        b["_meta"] = dict()
        b["_meta"]["core"] = True
        core_biomols.append(b)

    print(f'Inserting core biomolecules {len(core_biomols)}')
    target["biomolecules"].insert_many(core_biomols)


def load_core_interactions(source, target):
    core_assocs = list()
    missing_biomolecules = list()
    for a in list(source["interactions"].find()):
        a["_meta"] = dict()
        a["_meta"]["core"] = True
        a["participants"] = list()
        a["participants"] = a["biomolecules"]
        del a["biomolecules"]
        core_assocs.append(a)

    print(f'Inserting core interactions {len(core_assocs)}')
    target["interactions"].insert_many(core_assocs)

    return missing_biomolecules


def load_core_experiments(source, target):
    core_experiments = list()
    for e in list(source["experiments"].find()):
        e["_meta"] = dict()
        e["_meta"]["core"] = True
        core_experiments.append(e)

    print(f'Inserting core experiments {len(core_experiments)}')
    target["experiments"].insert_many(core_experiments)


def execute(config, database_manager):
    pipeline_config = config["dependencies"]["core"]
    source_host = pipeline_config["source"]["host"]
    source_port = pipeline_config["source"]["port"]
    source_database = pipeline_config["source"]["database"]

    target_host = pipeline_config["target"]["host"]
    target_port = pipeline_config["target"]["port"]
    target_database = pipeline_config["target"]["database"]

    source_connection = database_manager.get_connection(
        database_name=source_database,
        host=source_host,
        port=source_port
    )
    target_connection = database_manager.get_connection(
        database_name=target_database,
        host=target_host,
        port=target_port
    )

    load_core_biomolecules(config, source_connection, target_connection)
    load_core_interactions(source_connection, target_connection)
    load_core_experiments(source_connection, target_connection)
