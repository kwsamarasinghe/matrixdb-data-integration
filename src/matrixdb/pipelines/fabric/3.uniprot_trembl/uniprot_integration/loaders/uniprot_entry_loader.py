import os
import time

from pymongo import MongoClient
import json, pandas

database = 'matrixdb-data-fabric'

CONFIG_FILE = "./conf/config.json"

with open(CONFIG_FILE) as config:
    app_config = json.load(config)
    # parsed data
    uniprot_entry_location = app_config["external"]["uniprot"]["output_location"]


CONNECTION_STRING = "mongodb://localhost:27018/"
db_connection = MongoClient(CONNECTION_STRING)[database]

entries_loaded = 0
start_time = time.time()

print("Loading uniprot entries")
for file in os.listdir(uniprot_entry_location):
    with open(uniprot_entry_location + file) as entry_file:
        data = json.load(entry_file)

        db_connection["uniprotEntries"].insert_many(list(e["entry"] for e in data))
        print("Loaded a batch of " + str(len(data)))
        entries_loaded += len(data)

print("Entries laoded " + str(entries_loaded))
print("--- Elapsed time %s seconds ---" % (time.time() - start_time))