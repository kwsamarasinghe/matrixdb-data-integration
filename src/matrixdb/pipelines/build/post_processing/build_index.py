import concurrent
from concurrent.futures import wait
import json
import time
from src.matrixdb.dataintegration.commons.database_connection_provider import get_connection

pre_prod_connection = get_connection("matrixdb_4_0")
pre_prod_connection_2 = get_connection("matrixdb-4_0-pre-prod")
fabric_connection = get_connection("matrixdb-data-fabric")


biomolecule_index_docs = list()
start = time.time()

meta_data_cache = {
    "psimi": dict(),
    "go": dict(),
    "interpro": dict(),
    "uniprotKeywords": dict(),
    "uberon": dict(),
    "bto": dict()
}

def build_biomolecule_registry():
    biomolecules = list()
    for biomolecule in pre_prod_connection["biomolecules"].find():
        biomolecules.append(biomolecule["id"])

    return sorted(biomolecules)


def build_meta_data_cache():
    # psimi
    for psimi in pre_prod_connection["psimi"].find():
        meta_data_cache["psimi"][psimi["id"]] = psimi

    # go
    for go in pre_prod_connection["go"].find():
        meta_data_cache["go"][go["id"]] = go

    # Interpro
    for interpro in pre_prod_connection["interpro"].find():
        meta_data_cache["interpro"][interpro["id"]] = interpro

    # Uniprot keywords
    for uniprot_keyword in pre_prod_connection["uniprotKeywords"].find():
        meta_data_cache["uniprotKeywords"][uniprot_keyword["id"]] = uniprot_keyword

    for uberon in pre_prod_connection["uberon"].find():
        meta_data_cache["uberon"][uberon["id"]] = uberon

    for bto in pre_prod_connection["brenda"].find():
        meta_data_cache["bto"][bto["id"]] = bto

build_meta_data_cache()

# Get number of interactions per biomolecule
count_cache = dict()
for stat in pre_prod_connection["statistics"].find():
    if stat["category"] == "interaction_counts_by_biomolecule":
        count_cache = stat["statistics"]

for biomolecule in pre_prod_connection["biomolecules"].find():
    index_document = dict()
    index_document["biomolecule_id"] = biomolecule["id"]
    index_document["biomolecule_type"] = biomolecule["type"]
    index_document["interaction_count"] = 0

    if biomolecule['id'] == 'O75342':
        print()
    if biomolecule['type'] == 'protein':
        if 'relations' in biomolecule:
            if 'gene_name' in biomolecule['relations']:
                if type(biomolecule["relations"]["gene_name"]) == list and len(biomolecule["relations"]["gene_name"]) > 0:
                    index_document["gene"] = ','.join(biomolecule["relations"]["gene_name"])
                else:
                    index_document["gene"] = biomolecule["relations"]["gene_name"]

    if 'CHEBI:' in biomolecule['id']:
        continue

    if "names" in biomolecule:
        if "name" in biomolecule["names"]:
            index_document["name"] = biomolecule["names"]["name"]

        if "common_name" in biomolecule["names"]:
            if type(biomolecule["names"]["common_name"]) is list:
                index_document["common_name"] = biomolecule["names"]["common_name"][0]
            else:
                index_document["common_name"] = biomolecule["names"]["common_name"]

        if "recommended_name" in biomolecule["names"]:
            index_document["recommended_name"] = biomolecule["names"]["recommended_name"]
            index_document["recommended_name_exact"] = biomolecule["names"]["recommended_name"]

        other_names = None
        if "other_names" in biomolecule["names"]:
            other_names = biomolecule["names"]["other_names"]

        if "other_name" in biomolecule["names"]:
            other_names = biomolecule["names"]["other_name"]

        if other_names is not None:
            if type(other_names) is list:

                other_names_to_combine = list()
                for other_name in other_names:
                    if type(other_name) is not dict:
                        other_names_to_combine.append(other_name)
                    else:
                        if 'fullName' in other_name:
                            if 'value' in other_name['fullName']:
                                other_names_to_combine.append(other_name['fullName']['value'])

                index_document["other_name"] = ','.join(other_names_to_combine)
            else:
                index_document["other_name"] = other_names

    if "description" in biomolecule:
        if type(biomolecule["description"]) is list:
            index_document["description"] = ",".join(biomolecule["description"])
        else:
            index_document["description"] = biomolecule["description"]

    if "annotations" in biomolecule:
        index_document["keyword_ids"] = list()
        index_document["keyword_names"] = list()

        if "keywords" in biomolecule["annotations"]:
            if type(biomolecule["annotations"]["keywords"]) is list:
                for keyword in biomolecule["annotations"]["keywords"]:
                    if "id" in keyword:
                        index_document["keyword_ids"].append(keyword["id"])
                    if "text" in keyword:
                        index_document["keyword_names"].append(keyword["text"])
            else:
                keyword = biomolecule["annotations"]["keywords"]
                if "id" in keyword:
                    index_document["keyword_ids"].append(keyword["id"])
                if "text" in keyword:
                    index_document["keyword_names"].append(keyword["text"])

            index_document['keyword_ids'] = ';'.join(index_document['keyword_ids'])
            index_document['keyword_names'] = ';'.join(index_document['keyword_names'])

        if "go" in biomolecule["annotations"]:
            index_document["go_ids"] = list()
            index_document["go_names"] = list()
            if type(biomolecule["annotations"]["go"]) is list:
                for go in biomolecule["annotations"]["go"]:
                    if type(go) == str:
                        index_document["go_ids"].append(go)
                        index_document["go_names"].append(meta_data_cache['go'][go]['term'])
                    elif type(go) == dict:
                        index_document["go_ids"].append(go['id'])
                        index_document["go_names"].append(meta_data_cache['go'][go['id']]['term'])

            index_document['go_ids'] = ';'.join(index_document['go_ids'])
            index_document['go_names'] = ';'.join(index_document['go_names'])

    if "molecular_details" in biomolecule:
        if "structure" in biomolecule["molecular_details"]:
            index_document["molecular_details"] = biomolecule["molecular_details"]["structure"]
        #if "sequence" in biomolecule["molecular_details"]:
        #    index_document["molecular_details"] = biomolecule["molecular_details"]["sequence"]

    if "species" in biomolecule:
        if type(biomolecule["species"]) == dict:
            index_document["species"] = biomolecule["species"]["id"]
        else:
            index_document["species"] = biomolecule["species"]


    xrefs = list()
    if "xrefs" in biomolecule:
        if "chebi" in biomolecule["xrefs"]:
            index_document['chebi'] = biomolecule["xrefs"]["chebi"]
        if "complex_portal" in biomolecule["xrefs"]:
            index_document['complex_portal'] = biomolecule["xrefs"]["complex_portal"]
        if "glytoucan" in biomolecule["xrefs"]:
            index_document['glytoucan'] = biomolecule["xrefs"]["glytoucan"]
        if "kegg" in biomolecule["xrefs"]:
            index_document['kegg'] = biomolecule["xrefs"]["kegg"]
        if "reactome" in biomolecule["xrefs"]:
            index_document['reactome'] = ';'.join([r['id'] for r in biomolecule["xrefs"]["reactome"]])
        if "interpro" in biomolecule["xrefs"]:
            index_document['interpro'] = ';'.join([meta_data_cache['interpro'][i]['name'] for i in biomolecule["xrefs"]["interpro"] if
                          i in meta_data_cache['interpro']])

    if len(xrefs) > 0:
        index_document["xrefs"] = ','.join(xrefs)

    # Interaction count
    if biomolecule["id"] in count_cache:
        index_document["interaction_count"] = count_cache[biomolecule["id"]]

    biomolecule_index_docs.append(index_document)

with open("biomol_index", "w") as biomol_index:
    biomol_index.write(json.dumps(biomolecule_index_docs))

print(f"Biomol index generated: Elapsed time {time.time() - start}")

'''

def process_pubmeds(pubmeds, batch_number, source_connection):
    print(f'Proessing batch {batch_number} {len(pubmeds)}')
    publications = list(source_connection['publications'].find({
        'id': {
            '$in': pubmeds
        }
    }))
    publication_index_docs = list()
    for publication in publications:
        index_document = dict()
        index_document["publication_id"] = publication["id"]
        if "abstract" in publication:
            index_document["abstract"] = publication["abstract"]

        if "title" in publication:
            index_document["title"] = publication["title"]

        index_document["journal"] = publication["journal"]

        if "authors" in publication:
            index_document["authors"] = ','.join(publication["authors"])

        # Calculate the number of interactions for each publication
        all_interactions = 0
        for experiment in list(source_connection["experiments"].find({'pmid': publication['id']})):
            interactions = len(list(source_connection["interactions"].find({
                '$or': [
                    {'experiments.direct.binary': experiment['id']},
                    {'experiments.direct.spoke_expanded_from': experiment['id']}
                ]
            })))
            all_interactions += interactions

        index_document["interaction_count"] = all_interactions

        publication_index_docs.append(index_document)

    with open(f'publication_index_{batch_number}', "w") as publication_index:
        publication_index.write(json.dumps(publication_index_docs))

    print(f'Batch {batch_number} finished')

num_threads = 10

# Function to process pubmeds using threads
# Split the list into chunks for parallel processing
publications = list(p['id'] for p in pre_prod_connection["publications"].find())
chunk_size = len(publications) // num_threads
pubmed_chunks = [publications[i:i + chunk_size] for i in range(0, len(publications), chunk_size)]

# Execute in parallel with batch numbers
with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
    futures = list()
    for batch_number, pubmeds in enumerate(pubmed_chunks, start=1):
        futures.append(executor.submit(process_pubmeds, pubmeds, batch_number, pre_prod_connection))
    wait(futures)

print(f"Publication index generated: Elapsed time {time.time() - start}")
'''
