'''
Loads the pubmed data from 3.5 ace files to the fabric
To be done once
'''
import json
import re

from src.matrixdb.dataintegration.commons.database_connection_provider import get_connection

CONFIG_FILE = "./conf/config.json"

fabric_connection = get_connection("matrixdb-data-fabric")

with open(CONFIG_FILE) as config:
    app_config = json.load(config)

    matrixdb_location = app_config["ace"]["location"]
    pubmed_ace_files = app_config["ace"]["external"]["pubmed"]


file = matrixdb_location + "/" + pubmed_ace_files + ".ace"
with open(file) as file:
    publication_list = []

    publication = dict()
    for line in file:
        if len(line) == 1:
            continue

        if re.match("//", line):
            publication_list.append(pubmed)
            continue

        if line.startswith('Publication'):
            splitted = re.split(" ", line)
            pubmed = splitted[1].strip()

            if len(publication.keys()) > 0:
                publication_list.append(publication)

            # Initialize the new biomolecule
            publication = dict()
            publication["id"] = pubmed.strip()
        else:
            key = None
            value = None

            match = re.compile(r'(Publication|Title|Author|Date|Abstract|Journal) "?([^"]+)"?').search(line)

            if match is not None:
                key = match.group(1).strip()
                value = match.group(2).strip()

                key = key.lower()
                if key is not None and value is not None:
                    if key in publication:
                        current = publication[key]
                        publication[key] = []
                        if type(current) == list:
                            for c in current:
                                publication[key].append(c)
                        else:
                            publication[key].append(current)
                            publication[key].append(value)
                    else:
                        publication[key] = value
            else:
                continue
    publication_list.append(publication)

fabric_connection["pubmed"].insert_many(publication_list)