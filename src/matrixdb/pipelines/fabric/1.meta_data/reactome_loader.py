'''
Loads the pubmed data from 3.5 ace files to the fabric
To be done once
'''
import json
import re

from src.matrixdb.pipeline_manager.connection_provider import get_connection

CONFIG_FILE = "./config/fabric_config.json"

target_connection = get_connection("mongodb://localhost:27018", "matrixdb-data-fabric")

with open(CONFIG_FILE) as config:
    app_config = json.load(config)
    reactome_file = app_config["dependencies"]["reactome"]["terms"]
    reactomr_complexes_file = app_config["dependencies"]["reactome"]["complexes"]

with open(reactome_file) as file:
    parsed_results = []

    for line in file:
        # Split the line by tabs or multiple spaces
        parts = line.split('\t')

        # Ensure the line has exactly 3 parts
        if len(parts) == 3:
            identifier, description, species = parts

            # Create a dictionary with the parsed values
            parsed_entry = {
                'id': identifier,
                'term': description,
                'species': species
            }

            # Append the dictionary to the results list
            parsed_results.append(parsed_entry)
target_connection["reactome"].insert_many(parsed_results)

with open(reactomr_complexes_file) as file:
    parsed_results = []
    for line in file:
        parts = line.split('\t')
        id = parts[0]
        name = parts[1]

        parsed_results.append({
            "id": id,
            "term": name
        })

target_connection["reactome"].insert_many(parsed_results)


#print(parsed_results)
