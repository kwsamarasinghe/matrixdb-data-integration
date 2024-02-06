import os
import re
import time
import requests
from requests.adapters import HTTPAdapter, Retry
from pymongo import MongoClient
import json

database = 'matrixdb-data-fabric'

CONFIG_FILE = "./conf/config.json"

with open(CONFIG_FILE) as config:
    app_config = json.load(config)
    # parsed data
    trembl_output_location = app_config["external"]["trembl"]["download_location"]

CONNECTION_STRING = "mongodb://localhost:27018/"
db_connection = MongoClient(CONNECTION_STRING)[database]

entries_loaded = 0
start_time = time.time()

re_next_link = re.compile(r'<(.+)>; rel="next"')
retries = Retry(total=5, backoff_factor=0.25, status_forcelist=[500, 502, 503, 504])
session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=retries))


def get_next_link(headers):
    if "Link" in headers:
        match = re_next_link.match(headers["Link"])
        if match:
            return match.group(1)


def get_batch(batch_url):
    while batch_url:
        response = session.get(batch_url)
        response.raise_for_status()
        total = response.headers["x-total-results"]
        yield response, total
        batch_url = get_next_link(response.headers)

filter_species_list = [
    "39053"
]
start_time = time.time()

for species_id in filter_species_list:
    url = "https://rest.uniprot.org/uniprotkb/search?query=organism_id%3A"+species_id+"%20AND%20%28reviewed%3Afalse%29&size=500&format=json"
    query_params = {
        "query": f"reviewed:false AND organism_id:{species_id}",
        "format": "json",
        "size": "500"
    }
    print(f'Fetching trembl entries for species {species_id}' )
    downloaded = 0
    for batch, total in get_batch(url):
        data = json.loads(batch.text)["results"]
        downloaded += len(data)
        print(f'{downloaded} / {total}')
        print(f'Loading {downloaded} entries , species : {species_id}')
        db_connection["tremblEntries"].insert_many(data)
'''
print("Loading trembl entries")
for file in os.listdir(trembl_output_location):
    with open(trembl_output_location + file) as entry_file:
        data = json.load(entry_file)["results"]

        #db_connection["tremblEntries"].insert_many(list(e["entry"] for e in data))
        print("Loaded a batch of " + str(len(data)))
        entries_loaded += len(data)
'''

print("--- Elapsed time %s seconds ---" % (time.time() - start_time))