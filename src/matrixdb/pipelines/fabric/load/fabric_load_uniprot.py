import time
from lxml import etree
import gzip

xmlns="{http://uniprot.org/uniprot}"

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


def parse_single_element(element):
    element_name = element.tag.replace(xmlns, "")
    element_json = {
        element_name: dict()
    }
    for name, value in element.attrib.items():
        element_json[element_name][name] = value

    if element.text is not None and len(element.text.strip()):
        element_json[element_name]["text"] = element.text

    if element_name[-len("List"):] == "List":
        element_json[element_name] = list()

    return element_json


def load_uniprot_entries(uniprot_file, target_database_connection):
    start_time = time.time()

    ## Uniprot distributes a single file in gz format
    uniprot_file = gzip.open(uniprot_file)
    context = etree.iterparse(uniprot_file, events=('start', 'end'))

    stack = list()
    entries = list()
    all_entries = 0
    all_uniprot = 0
    for event, element in context:

        if event == "start":
            parsed_element = parse_single_element(element)
            stack.append(parsed_element)

        if event == "end":
            if len(stack) > 1:

                current = stack.pop()
                # To handle text elements
                if element.text and len(element.text.strip()):
                    current = parse_single_element(element)

                parent = stack.pop()

                parent_name = list(filter(lambda k: k != "attributes", parent.keys()))[0]
                current_name = list(filter(lambda k: k != "attributes", current.keys()))[0]

                if isinstance(parent[parent_name], list):
                    parent[parent_name].append(current[current_name])
                elif isinstance(parent[parent_name], dict):
                    if current_name in parent[parent_name]:
                        existing_current = parent[parent_name][current_name]
                        if not isinstance(existing_current, list):
                            parent[parent_name][current_name] = [existing_current]
                        parent[parent_name][current_name].append(current[current_name])
                    else:
                        parent[parent_name][current_name] = current[current_name]

                stack.append(parent)
            elif len(stack) == 1:
                current = stack.pop()

            if 'entry' in current:
                all_uniprot += 1
                if current["entry"]["organism"]["dbReference"]["id"] in filter_species_list:
                    entries.append(current)
                    all_entries += 1
                else:
                    del current
                for s in stack:
                    del s
                stack = []

        element.clear()
        while element.getprevious() is not None:
            del element.getparent()[0]


        if len(entries) == 10000:
            print("Loading 10000 entries ..")
            target_database_connection["uniprotEntries"].insert_many(list(e["entry"] for e in entries))
            for e in entries:
                del e
            for s in stack:
                del s
            entries = list()
            stack = list()

    if len(entries) > 0:
        target_database_connection["uniprotEntries"].insert_many(list(e["entry"] for e in entries))
        for e in entries:
            del e
        for s in stack:
            del s
        entries = list()
        stack = list()

    print("Filtered " + str(all_entries) + " entries out of " + str(all_uniprot))
    print("--- Elapsed time %s seconds ---" % (time.time() - start_time))


def execute(config, database_manager):
    uniprot_raw_file = config["dependencies"]["uniprot"]["source"]["location"]

    target_host = config["dependencies"]["uniprot"]["target"]["host"]
    target_port = config["dependencies"]["uniprot"]["target"]["port"]
    target_database = config["dependencies"]["uniprot"]["target"]["database"]

    target_connection = database_manager.get_connection(
        database_name=target_database,
        host=target_host,
        port=target_port
    )

    load_uniprot_entries(uniprot_raw_file, target_connection)