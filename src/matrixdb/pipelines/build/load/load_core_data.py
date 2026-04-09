import logging
import math

import pandas as pd
import requests
import re

# Function to make an HTTP call and parse the response to JSON
from src.matrixdb.pipeline_manager.database_manager import DatabaseManager

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

    complex_portal_annotations['stochiometry'] = list()
    for participant in complex_portal_data['participants']:
        stochiometry = {
            "id": participant["identifier"]
        }
        if "stochiometry" in participant and participant["stochiometry"] is not None:
            splits = participant["stochiometry"].split(',')
            if len(splits) > 0:
                stochiometry["min"] = splits[0].split(":")[1]
            if len(splits) > 1:
                stochiometry["max"] = splits[1].split(":")[1]
        complex_portal_annotations['stochiometry'].append(stochiometry)

    return complex_portal_annotations


def load_multimers(multimers, source, target):
    """
        Loads multimers either from the database, if exists
        complementarity, if does not exist in the database, create a new one and get additional data from complex portal
        """
    multimers_to_insert = list()
    # Read and get the list of multimers from the custom biomolecule file
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
                multimers_to_insert.append(existing_multimer)
                species = re.compile(r'[0-9]+').search(multimer_row[3]).group(0)
                existing_multimer["species"] = {
                    "db": "NCBI Taxonomy",
                    "id": species
                }
                continue

            if "complex_portal" not in existing_multimer["xrefs"]:
                multimers_to_insert.append(existing_multimer)
                species = re.compile(r'[0-9]+').search(multimer_row[3]).group(0)
                existing_multimer["species"] = {
                    "db": "NCBI Taxonomy",
                    "id": species
                }
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

            complex_portal_id = multimer_row[4]
            comments = multimer_row[5]
            if comments == '':
                comments = complex_portal_data["description"]
            other_info = multimer_row[6]

            new_multimer = {
                "id": id,
                "type": 'multimer',
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

        species = re.compile(r'[0-9]+').search(multimer_row[3]).group(0)
        new_multimer["species"] = {
            "db": "NCBI Taxonomy",
            "id": species
        }

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

            new_multimer["molecular_details"] = {
                "stochiometry": complex_portal_data["stochiometry"]
            }
        else:
            # Restructure stochiometry data
            if "molecular_details" in new_multimer:
                if "stoichiometry" in new_multimer["molecular_details"]:
                    sto = new_multimer["molecular_details"]["stoichiometry"]
                    new_multimer["molecular_details"]["stochiometry"] = dict()
                    for participant in sto.split("+"):
                        participant = participant.trim()
                        min = participant.split(" ")[0]
                        identifier = participant.split(" ")[1]
                        new_multimer["molecular_details"]["stochiometry"].append({
                            "id": identifier,
                            "min": min
                        })
                    del new_multimer["molecular_details"]["stoichiometry"]

            logging.warning({
                "message": f'No complex portal data for {new_multimer["id"]}'
            })

        if "_meta" in new_multimer:
            new_multimer["_meta"]["core"] = True

        multimers_to_insert.append(new_multimer)

    print(f'Inserting core biomolecules {len(multimers_to_insert)}')
    target["biomolecules"].insert_many(multimers_to_insert)


def load_pfrags(pfrags, source, target):
    pfrags_to_insert = list()
    for row_id, pfrag_row in pfrags.iterrows():
        species = re.compile(r'[0-9]+').search(pfrag_row[3]).group(0)
        pfrag_id = pfrag_row[0]
        existing_pfrag = source["biomolecules"].find_one({
            "id": pfrag_id
        })

        if existing_pfrag is None:
            print(f'{pfrag_id} does not exist')
        else:
            existing_pfrag["species"] = {
                "db": "NCBI Taxonomy",
                "id": species,
            }
        pfrags_to_insert.append(existing_pfrag)

    target["biomolecules"].insert_many(pfrags_to_insert)
    return 0


def load_gags(gags, source, target):
    gags_to_insert = list()
    for row_id, gag in gags.iterrows():
        existing_gag = source['biomolecules'].find_one({
            'id': gag[0]
        })
        if existing_gag is not None:
            del existing_gag["_id"]
            for xref in gag[4].split(','):
                if 'GlyTouCan' in xref:
                    existing_gag['xrefs']['glytoucan'] = xref.split(':')[1]
            gags_to_insert.append(existing_gag)
        else:
            xrefs = dict()
            for xref in gag[4].split(','):
                if 'GlyTouCan' in xref:
                    xrefs['glytoucan'] = xref.split(':')[1]
                if 'CHEBI' in xref:
                    xrefs['chebi'] = xref.split(':')[1]
                if 'KEGG' in xref:
                    xrefs['kegg'] = xref.split(':')[1]

            gags_to_insert.append({
                'id': gag[0],
                'type': 'gag',
                'names': {
                    'name': gag[1]
                },
                'xrefs': xrefs,
                'description': gag[5],
                '_meta': {
                    'core': True
                }
            })

    print(f'Loading gags {len(gags_to_insert)}')
    target["biomolecules"].insert_many(gags_to_insert)


def load_smallmols(smallmols, source, target):
    smallmols_to_insert = list()
    for row_id, smallmol in smallmols.iterrows():
        existing_smallmol = source['biomolecules'].find_one({
            'id': smallmol[0]
        })
        smallmols_to_insert.append(existing_smallmol)
    target["biomolecules"].insert_many(smallmols_to_insert)


def load_speps(speps, source, target):
    speps_to_insert = list()
    for row_id, spep in speps.iterrows():
        existing_spep = source["biomolecules"].find_one({
            "id": spep[0]
        })
        speps_to_insert.append(existing_spep)
    target['biomolecules'].insert_many(speps_to_insert)


def load_lipids(lipids, source, target):
    lipids_to_insert = list()
    for row_id, lipid in lipids.iterrows():
        existing_lipid = source["biomolecules"].find_one({
            "id": lipid[0]
        })
        lipids_to_insert.append(existing_lipid)
    target['biomolecules'].insert_many(lipids_to_insert)


def load_cat(cats, source, target):
    cats_to_insert = list()
    for row_id, cat in cats.iterrows():
        existing_cat = source["biomolecules"].find_one({
            "id": cat[0]
        })
        cats_to_insert.append(existing_cat)
    target['biomolecules'].insert_many(cats_to_insert)


def load_core_biomolecules(config, source, target):
    core_biomols = list()
    # Read and get the list of multimers from the custom biomolecule file
    df = pd.read_csv(config["dependencies"]["core"]["raw"], dtype='str', sep='\t')

    # Multimers
    multimers = df[df['MatrixDB identifier'].str.contains("MULT_", case=False, na=False)]
    logging.info(f"Loading multimers : {len(multimers)}")
    load_multimers(multimers, source, target)

    # Pfrags
    pfrags = df[df['MatrixDB identifier'].str.contains("PFRAG_", case=False, na=False)]
    logging.info(f"Loading pfrags {len(pfrags)}")
    load_pfrags(pfrags, source, target)

    # GAG
    gags = df[df['MatrixDB identifier'].str.contains("GAG_", case=False, na=False)]
    logging.info(f"Loading gags {len(gags)}")
    load_gags(gags, source, target)

    # smallmol
    smallmols = df[df['MatrixDB identifier'].str.contains("SMALLMOL_", case=False, na=False)]
    logging.info(f"Loading smallmols {len(smallmols)}")
    #load_smallmols(smallmols, source, target)

    # lipid
    lipids = df[df['MatrixDB identifier'].str.contains("LIP_", case=False, na=False)]
    logging.info(f"Loading lipids {len(lipids)}")
    #load_lipids(lipids, source, target)

    # cat
    cats = df[df['MatrixDB identifier'].str.contains("CAT_", case=False, na=False)]
    logging.info(f"Loading cats {len(cats)}")
    #load_cat(cats, source, target)

    #spep
    speps = df[df['MatrixDB identifier'].str.contains("SPEP_", case=False, na=False)]
    logging.info(f"Loading speps {len(speps)}")
    #load_speps(speps, source, target)


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
    '''

    pipeline_config = config["dependencies"]["core"]
    source_host = pipeline_config["source"]["host"]
    source_port = pipeline_config["source"]["port"]
    source_database = pipeline_config["source"]["database"]

    target_host = pipeline_config["target"]["host"]
    target_port = pipeline_config["target"]["port"]
    target_database = pipeline_config["target"]["database"]

    '''
    #pipeline_config = config["dependencies"]["core"]
    source_host = 'localhost'
    source_port = '27018'
    source_database = 'matrixdb-4_0-pre-prod'

    target_host = 'localhost'
    target_port = '27018'
    target_database = 'matrixdb_4_0'

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

    '''
    core_interactions = list(source_connection['interactions'].find({
        '_meta.core': True
    }))

    # Only load the relevant experiments
    experiments = set()
    for core_interaction in core_interactions:
        if 'directly_supported_by' in core_interaction:
            experiments.update(list(d for d in core_interaction['directly_supported_by']))
        if 'spoke_expanded_from' in core_interaction:
            experiments.update(list(s for s in core_interaction['spoke_expanded_from']))

    core_experiments = list(source_connection['experiments'].find({
        'id':  {
            '$in': list(experiments)
        }
    }))

    target_connection['coreInteractions'].insert_many(core_interactions)
    target_connection['coreExperiments'].insert_many(core_experiments)
    '''

    load_core_biomolecules(config, source_connection, target_connection)
    #load_core_interactions(source_connection, target_connection)
    #load_core_experiments(source_connection, target_connection)


if __name__ == '__main__':
    database_manager = DatabaseManager()
    execute('', database_manager)