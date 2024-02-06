import json
import xml.etree.ElementTree as ET
from pymongo import MongoClient

owl_ns = "{http://www.w3.org/2002/07/owl#}"
rdf_ns = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}"
rdfs_ns = "{http://www.w3.org/2000/01/rdf-schema#}"
obo_owl_ns = '{http://www.geneontology.org/formats/oboInOwl#}'
obo1_ns = '{http://purl.obolibrary.org/obo/}'
CONFIG_FILE = "./conf/config.json"


def get_db_connection(database):
    connection_string = "mongodb://localhost:27018/"
    return MongoClient(connection_string)[database]


with open(CONFIG_FILE) as config:
    app_config = json.load(config)
    go_file_location = app_config["external"]["go"]["file"]

    with open(go_file_location) as psimi_file:
        tree = ET.parse(psimi_file)
        root = tree.getroot()

        int_db = False
        go_to_load = list()
        for go_class in root.findall(owl_ns + "Class"):
            hasNamespaceElement = go_class.find(obo_owl_ns + "hasOBONamespace")
            if hasNamespaceElement is not None:
                if hasNamespaceElement.text == 'gene_ontology' or hasNamespaceElement.text == 'biological_process' or hasNamespaceElement.text == 'cellular_component' or hasNamespaceElement.text ==  'molecular_function':
                    label_element = go_class.find(rdfs_ns + "label")
                    if label_element is not None:
                        term_label = label_element.text
                        term_id = go_class.find(obo_owl_ns + "id").text
                        description = go_class.find(obo1_ns + "IAO_0000115").text
                        category = go_class.find(obo_owl_ns + "hasOBONamespace").text
                        go_term = {
                            "id": term_id,
                            "term": term_label,
                            "description": description,
                            "category": category
                        }

                        subclass_elemnent =  go_class.find(rdfs_ns + "subClassOf")
                        if subclass_elemnent is not None:
                            if f'{rdf_ns}resource' in subclass_elemnent.attrib:
                                go_term["subclass_of"] = subclass_elemnent.attrib[f'{rdf_ns}resource'].split('/')[-1]

                        go_to_load.append(go_term)

                    else:
                        print()

        data_fabric_connection = get_db_connection("matrixdb-data-fabric")
        data_fabric_connection["go"].insert_many(go_to_load)
