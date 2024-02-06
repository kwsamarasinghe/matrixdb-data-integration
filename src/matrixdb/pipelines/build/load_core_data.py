import json

from pymongo import MongoClient

def get_db_connection(database):
    CONNECTION_STRING = "mongodb://localhost:27018/"
    return MongoClient(CONNECTION_STRING)[database]

CONFIG_FILE = "./conf/config.json"
with open(CONFIG_FILE) as config:
    app_config = json.load(config)
    pre_prod_database_name = app_config["dist"]["mongodb"]["name"]

core_connection = get_db_connection("matrixdb-core")
pre_prod_connection = get_db_connection(pre_prod_database_name)

core_biomols = list()
for b in list(core_connection["biomolecules"].find()):
    b["_meta"] = dict()
    b["_meta"]["core"] = True
    core_biomols.append(b)

core_assocs = list()
for a in list(core_connection["interactions"].find()):
    a["_meta"] = dict()
    a["_meta"]["core"] = True
    a["participants"] = list()
    a["participants"] = a["biomolecules"]
    del a["biomolecules"]
    core_assocs.append(a)

core_experiments = list()
for e in list(core_connection["experiments"].find()):
    e["_meta"] = dict()
    e["_meta"]["core"] = True
    core_experiments.append(e)

print(f'Inserting core biomolecules {len(core_biomols)}')
pre_prod_connection["biomolecules"].insert_many(core_biomols)

print(f'Inserting core interactions {len(core_assocs)}')
pre_prod_connection["interactions"].insert_many(core_assocs)

print(f'Inserting core experiments {len(core_experiments)}')
pre_prod_connection["experiments"].insert_many(core_experiments)