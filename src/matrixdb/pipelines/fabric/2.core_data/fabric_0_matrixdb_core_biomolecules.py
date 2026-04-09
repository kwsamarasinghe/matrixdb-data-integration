import json

from pymongo import MongoClient

CONFIG_FILE = "./conf/fabric_config.json"

def process_pfrag(pfrag_line):
    pfrag_id = pfrag_line.split('\t')[0]
    species = pfrag_line.split('\t')[3]

def process_mult(mult_line):
    multimer_id = mult_line.split('\t')[0]
    species = mult_line.split('\t')[3]


with open(CONFIG_FILE) as config:
    app_config = json.load(config)
    custom_biomolecules = app_config["dependencies"]["core"]["raw"]

    with open(custom_biomolecules, "r") as custom_biomolecules:
        for line in custom_biomolecules.readlines():
            columns = line.split('\t')
            biomolecule_id = columns[0]

            if 'MULT' in biomolecule_id:
                process_mult(line)

            if 'PFRAG' in biomolecule_id:
                process_pfrag(line)