import json
import time
from lxml import etree
import gzip

xmlns="{http://uniprot.org/uniprot}"


CONFIG_FILE = "./conf/config.json"

with open(CONFIG_FILE) as config:
    app_config = json.load(config)
    uniprot_file_location = app_config["uniprot"]["input_location"]
    uniprot_output_location = app_config["uniprot"]["output_location"]


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

start_time = time.time()

## Uniprot distributes a single file in gz format
uniprot_file = gzip.open(uniprot_file_location + "uniprot_sprot.xml.gz" )
context = etree.iterparse(uniprot_file, events=('start', 'end'))


stack = list()
entries = list()
all_entries = 0
all_uniprot = 0
batch_no = 1
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
        print("writing a batch of 10000 to a file : batch " + str(batch_no))
        with open(uniprot_output_location + "/uniprot-entries-" + str(batch_no) + "-" + str(time.time()) + ".json",
                  "w") as uniprot_file:
            uniprot_file.write(json.dumps(entries))
        for e in entries:
            del e
        for s in stack:
            del s
        entries = list()
        stack = list()
        batch_no += 1


print("Filtered " + str(all_entries) + " entries out of " + str(all_uniprot))
print("--- Elapsed time %s seconds ---" % (time.time() - start_time))