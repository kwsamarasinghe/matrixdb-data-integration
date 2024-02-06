import json
import xml.etree.ElementTree as ET
from pymongo import MongoClient

owl_ns = "{http://www.w3.org/2002/07/owl#}"
rdf_ns = "{http://www.w3.org/2000/01/rdf-schema#}"
obo_owl_ns = '{http://www.geneontology.org/formats/oboInOwl#}'

CONFIG_FILE = "./conf/config.json"


def get_db_connection(database):
    connection_string = "mongodb://localhost:27018/"
    return MongoClient(connection_string)[database]


with open(CONFIG_FILE) as config:
    app_config = json.load(config)
    psimi_file_location = app_config["external"]["bto"]["file"]

    with open(psimi_file_location) as psimi_file:
        tree = ET.parse(psimi_file)
        root = tree.getroot()

        int_db = False
        psimi_to_load = list()
        for annotation_property in root.findall(owl_ns + "Class"):
            label_element = annotation_property.find(rdf_ns + "label")
            if label_element is not None:
                term_label = label_element.text
                term_id = annotation_property.find(obo_owl_ns + "id").text
                psimi_to_load.append({
                    "id": term_id,
                    "name": term_label
                })
            else:
                print()


        data_fabric_connection = get_db_connection("matrixdb-data-fabric")
        data_fabric_connection["bto"].insert_many(psimi_to_load)
