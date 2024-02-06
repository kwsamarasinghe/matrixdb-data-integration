import json
import xml.etree.ElementTree as ET
from pymongo import MongoClient

owl_ns = "{http://www.w3.org/2002/07/owl#}"
rdf_ns = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}"
obo_owl_ns = '{http://www.geneontology.org/formats/oboInOwl#}'
rdfs_ns = "{http://www.w3.org/2000/01/rdf-schema#}"

CONFIG_FILE = "./conf/config.json"


def get_db_connection(database):
    connection_string = "mongodb://localhost:27018/"
    return MongoClient(connection_string)[database]


with open(CONFIG_FILE) as config:
    app_config = json.load(config)
    psimi_file_location = app_config["external"]["psimi"]["file"]

    with open(psimi_file_location) as psimi_file:
        tree = ET.parse(psimi_file)
        root = tree.getroot()

        int_db = False
        psimi_to_load = list()
        for annotation_property in root.findall(owl_ns + "Class"):
            hasNamespaceElement = annotation_property.find(obo_owl_ns + "hasOBONamespace")
            if hasNamespaceElement is not None:
                if hasNamespaceElement.text == 'PSI-MI':
                    label_element = annotation_property.find(rdfs_ns + "label")
                    if label_element is not None:
                        term_label = label_element.text
                        term_id = annotation_property.find(obo_owl_ns + "id").text
                        uberon_term = {
                            "id": term_id,
                            "name": term_label,
                        }

                        subclass_elemnent = annotation_property.find(rdfs_ns + "subClassOf")
                        if subclass_elemnent is not None:
                            if f'{rdf_ns}resource' in subclass_elemnent.attrib:
                                uberon_term["subclass_of"] = subclass_elemnent.attrib[f'{rdf_ns}resource'].split('/')[
                                    -1]

                        psimi_to_load.append(uberon_term)

                    else:
                        print()

        data_fabric_connection = get_db_connection("matrixdb-data-fabric")
        data_fabric_connection["psimi"].insert_many(psimi_to_load)
