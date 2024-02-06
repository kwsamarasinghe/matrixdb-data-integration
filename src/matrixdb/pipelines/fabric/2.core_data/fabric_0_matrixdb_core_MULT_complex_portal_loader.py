import pandas as pd
import json

import requests
from pymongo import MongoClient

CONFIG_FILE = "./conf/config.json"


def get_db_connection(database):
    CONNECTION_STRING = "mongodb://localhost:27018/"
    return MongoClient(CONNECTION_STRING)[database]

'''
Enhance multimer data with complex portal data
'''
complex_portal_API = 'https://www.ebi.ac.uk/intact/complex-ws/complex/'

complex_portal_api = 'https://www.ebi.ac.uk/intact/complex-ws/complex/'

# Function to make an HTTP call and parse the response to JSON
def fetch_data(cpx_id):
    try:
        response = requests.get(f'{complex_portal_api}{cpx_id}')

        # Check if the request was successful (status code 200)
        response.raise_for_status()

        # Parse the response to JSON
        data = response.json()
        print('Data from API:', data)

        return data

    except requests.exceptions.RequestException as err:
        print(f'Error fetching data: {err}')

# Read all multimers from core database
core_connection = get_db_connection("matrixdb-4_0-pre-prod")
for multimer in core_connection["biomolecules"].find({
    'type': 'multimer'
}):
    if "xrefs" not in multimer:
        continue

    if "complex_portal" not in multimer["xrefs"]:
        continue

    cpx = multimer["xrefs"]["complex_portal"]
    complex_portal_data = fetch_data(cpx)

    # Functions
    function = None
    if "functions" in complex_portal_data:
        function = complex_portal_data["functions"][0]

    go_terms = list()
    for go in list(filter(lambda xref: xref["database"] == 'gene ontology', complex_portal_data["crossReferences"])):
        go_terms.append({
            "id": go["identifier"]
        })

    pdbs = list()
    for pdb in list(filter(lambda xref: xref["database"] == 'wwpdb', complex_portal_data["crossReferences"])):
        pdbs.append(pdb["identifier"])

    if len(go)>0:
        result = core_connection["biomolecules"].update_one(
            {'id': multimer["id"]},
            {'$set': {'annotations.go': go_terms}}
        )

    if len(pdbs)>0:
        result = core_connection["biomolecules"].update_one(
            {'id': multimer["id"]},
            {'$set': {'xrefs.pdb': pdbs}}
        )

    if function is not None:
        result = core_connection["biomolecules"].update_one(
            {'id': multimer["id"]},
            {'$set': {'annotations.function.text': function}}
        )

    print()
