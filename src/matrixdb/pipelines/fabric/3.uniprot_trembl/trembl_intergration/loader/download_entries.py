import json
import time

import requests

filter_species_list = [
    "394",
    "984",
    "7955",
    "9606",
    "9615",
    "9823",
    "9913",
    "9986",
    "9940",
    "44689",
    "9031",
    "9615",
    "10090",
    "10116",
    "10144",
    "11686",
    "11696",
    "39053"
]

CONFIG_FILE = "./conf/config.json"
with open(CONFIG_FILE) as config:
    app_config = json.load(config)
    trembl_output_location = app_config["external"]["trembl"]["download_location"]

def fetch_uniprot_data(species_id):
    base_url = "https://rest.uniprot.org/uniprotkb/search"
    query_params = {
        "query": f"reviewed:false AND organism_id:{species_id}",
        "format": "json"
    }

    try:
        response = requests.get(base_url, params=query_params)
        response.raise_for_status()  # Raise an exception for any HTTP error
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for species {species_id}: {e}")
        return None


def main():

    for species_id in filter_species_list:
        json_data = fetch_uniprot_data(species_id)
        if json_data:
            print(f"Writing JSON data for species {species_id}\n")
            with open(trembl_output_location + species_id + "-" + str(time.time()) + ".json", "w") as species_file:
                species_file.write(json.dumps(json_data))


if __name__ == "__main__":
    main()
