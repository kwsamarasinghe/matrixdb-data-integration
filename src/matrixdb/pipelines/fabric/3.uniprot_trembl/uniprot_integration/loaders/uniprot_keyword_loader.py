import os

from pymongo import MongoClient
import json, pandas

database = 'matrixdb-data-fabric'

CONFIG_FILE = "./conf/config.json"

with open(CONFIG_FILE) as config:
    app_config = json.load(config)
    # parsed data
    keyword_file = app_config["external"]["uniprot_keywords"]

with open(keyword_file) as keyword_file:
    CONNECTION_STRING = "mongodb://localhost:27018/"
    db_connection = MongoClient(CONNECTION_STRING)[database]
    data = json.load(keyword_file)["results"]
    db_connection["uniprotKeywords"].insert_many(data)
    print("Keywords laoded " + len(data))