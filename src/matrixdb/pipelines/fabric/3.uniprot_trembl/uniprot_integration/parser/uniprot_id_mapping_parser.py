import json
import time
import gzip
from pymongo import MongoClient

xmlns="{http://uniprot.org/uniprot}"


CONFIG_FILE = "./conf/config.json"

with open(CONFIG_FILE) as config:
    app_config = json.load(config)
    uniprot_file_location = app_config["external"]["uniprot"]["input_location"]
    uniprot_output_location = app_config["external"]["uniprot"]["output_location"]


def get_db_connection(database):
    CONNECTION_STRING = "mongodb://localhost:27018/"
    return MongoClient(CONNECTION_STRING)[database]

start_time = time.time()

# Retrive the uniprot ids
fabric_connection = get_db_connection('matrixdb-pre-prod')
uniprot_ids = dict()
for u in fabric_connection["biomolecules"].find({'type': 'protein', 'dataset': 'Swiss-Prot'}):
    uniprot_ids[u["id"]] = 1

## Human id mapping file
human_mapping_count = 0
mapped_uniprot_humanids = set()
with gzip.open(uniprot_file_location + "HUMAN_9606_idmapping.dat.gz" ) as uniprot_human_file:
    for line in uniprot_human_file:
        line = line.decode('utf-8').strip()
        tabs = line.split('\t')
        uniprot_id = tabs[0]
        type = tabs[1]
        ensg_id = tabs[2]

        if 'Ensembl' == type:
            if uniprot_id in uniprot_ids:
                print(f'{uniprot_id} {ensg_id}')
                mapped_uniprot_humanids.add(uniprot_id)
                human_mapping_count += 1


print(f'Human mapping id count {human_mapping_count} {len(mapped_uniprot_humanids)}')
print("--- Elapsed time %s seconds ---" % (time.time() - start_time))