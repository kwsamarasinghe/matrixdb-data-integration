import json
import time
from pymongo import MongoClient

CONFIG_FILE = "./conf/config.json"

with open(CONFIG_FILE) as config:
    app_config = json.load(config)
    matrisome_proteomics_file = app_config["external"]["matrisome"]["file"]
    matrisome_tissue_taxonomy_file = app_config["external"]["matrisome"]["tissue_taxonomy"]
    matrisome_sample_taxonomy_file = app_config["external"]["matrisome"]["sample_taxonomy"]


CONNECTION_STRING = "mongodb://localhost:27018/"
db_connection = MongoClient(CONNECTION_STRING)["matrixdb-pre-prod"]

sample_taxonomy_map = dict()
header = True
with open(matrisome_sample_taxonomy_file) as sample_file:

    for line in sample_file:

        if header:
            header = False
            continue

        columns = line.strip('\n').split('\t')
        sample = columns[0]
        sample_taxonomy_map[sample] = {
            'brenda': columns[1],
            'uberon': columns[2],
            'fma': columns[3],
            'mondo': columns[4],
            'ncit': columns[5],
            'cl': columns[6],
            'oae': columns[7],
            'efo': columns[8]
        }

tissue_taxonomy_map = dict()
header = True
with open(matrisome_tissue_taxonomy_file) as tissue_file:
    for line in tissue_file:

        if header:
            header = False
            continue

        columns = line.strip('\n').split(',')
        tissue_taxonomy_map[columns[1]] = columns[0]

header = True
proteomics_expression = dict()
with open(matrisome_proteomics_file) as proteomics_file:
    for line in proteomics_file:

        if header:
            header = False
            continue

        columns = line.strip('\n').split('\t')
        uniprot = columns[3]
        if uniprot not in proteomics_expression:
            proteomics_expression[uniprot] = list()

        prot_expression = {
            'tissueId': tissue_taxonomy_map[columns[6]],
            'sourceData': columns[8],
            'confidenceScore': columns[10]
        }
        if columns[7] in sample_taxonomy_map:
            prot_expression["sampleName"] = columns[7]
            prot_expression['sample'] = sample_taxonomy_map[columns[7]]
        proteomics_expression[uniprot].append(prot_expression)

expressions_to_load = list()
for k in proteomics_expression:
    expressions_to_load.append({
        "uniprot": k,
        "expressions": proteomics_expression[k]
    })
db_connection["proteomicsExpressions"].insert_many(expressions_to_load)

print()