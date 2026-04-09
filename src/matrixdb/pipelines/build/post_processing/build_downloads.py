import re
from src.matrixdb.pipeline_manager.connection_provider import get_connection
import pandas as pd

core_connection = get_connection("mongodb://localhost:27018/", "matrixdb_4_0")


# All core paritciapnts
# gag
gags = dict()
for gag in list(core_connection["biomolecules"].find(
    {
        'type': 'gag'
    }
)):
    gags[gag["xrefs"]["chebi"]] = gag

multimers = dict()
for multimer in list(core_connection["biomolecules"].find({
    'type': 'multimer'
})):
    if "xrefs" in multimer:
        multimers[multimer["xrefs"]["complex_portal"]] = multimer
    else:
        print()

pfrags = dict()
for pfrag in list(core_connection["biomolecules"].find({
    'type': 'pfrag'
})):
    if "xrefs" in pfrag:
        pfrags[pfrag["xrefs"]["uniprot"]] = pfrag
    else:
        print()

smallmols = dict()
for smallmol in list(core_connection["biomolecules"].find({
    'type': 'smallmol'
})):
    smallmols[smallmol["xrefs"]["chebi"]] = smallmol

cats = dict()
for cat in list(core_connection["biomolecules"].find({
    'type': 'cat'
})):
    cats[cat["xrefs"]["chebi"]] = cat

lips = dict()
for lip in list(core_connection["biomolecules"].find({
    'type': 'lipid'
})):
    lips[lip["xrefs"]["chebi"]] = lip

proteins = dict()
for protein in list(core_connection["biomolecules"].find({
    'type': 'protein'
})):
    proteins[protein["id"]] = protein


ebi_ids = set(i['xrefs']['intact'] for i in core_connection["experiments"].find({}))

# All interactions PSIMI
def generate_all_interaction_psimitab():
    all_inf_file = open('all_interactions', 'w')
    with open('./data/intact-data.txt', 'r') as intact_file:
        first_line = True
        for line in intact_file:
            if first_line:
                first_line = False
                continue

            int_ids = line.split('\t')[13]
            pattern = r'EBI-\d+'
            found = re.findall(pattern, int_ids)
            if found:
                if found[0] in ebi_ids:
                    all_inf_file.write(line)

# Core interactions PSIMI
def generate_core_interaction_psimitab():

    with open('./data/matrixdb_CORE.tab', 'w') as core_file:
        line_number = 0
        int_count = 0
        for line in core_file:
            if line_number == 0:
                line_number = 1
                continue

            p1 = line.split('\t')[0]
            if len(line.split('\t')) > 1:
                p2 = line.split('\t')[1]
            else:
                print()

            if 'chebi' in p1:
                p1 = p1.replace('chebi:', '')

            if 'uniprotkb' in p1:
                p1 = p1.replace('uniprotkb:', '')

            if 'complex-portal' in p1:
                p1 = p1.replace('complex-portal:', '')

            if 'chebi' in p2:
                p2 = p2.replace('chebi:', '')

            if 'uniprotkb' in p2:
                p2 = p2.replace('uniprotkb:', '')

            if 'complex-portal' in p2:
                p2 = p2.replace('complex-portal:', '')

            p1 = p1.strip('"')
            invalid_p1 = True
            if 'matrixdb' in p1 or p1 in gags or p1 in smallmols or p1 in lips or p1 in cats or p1 in proteins:
                print(line)
                invalid_p1 = False

            p2 = p2.strip('"')
            invalid_p2 = True
            if 'matrixdb' in p2 or p2 in gags or p2 in smallmols or p2 in lips or p2 in cats or p2 in proteins:
                print(line)
                invalid_p2 = False

            if invalid_p1 or invalid_p2:
                print()

            print(line)
            int_count += 1




# All ecm proteins , either with matrixdb criteria and/or matrisome categories
def generate_ecm_file():

    # Interaction data
    interactions_counts = core_connection["statistics"].find_one({
        'category': 'interaction_counts_by_biomolecule'
    })['statistics']

    file_path = 'ecm_proteins.csv'

    go_mapping = {
        "GO:0031012": "Extracellular matrix",
        "GO:0005604": "Basement membrane",
        "GO:0005615": "Extracellular space",
        "GO:0005576": "Extracellular region"
    }

    kw_mapping = {
        "KW-0084": "Basement membrane",
        "KW-0272": "Extracellular matrix",
        "KW-0964": "Extracellular space/secreted"
    }

    # Open the file in write mode and write the header and line
    header = 'protein_id,species (NCBI),go_terms,uniprot_keywords,dataset,interaction_count\n'


    lines = list()
    for ecm_protein in core_connection["biomolecules"].find({
        '$and': [
            {
                '$or': [
                    {
                        'ecm': {
                            '$exists': True
                        }
                    },
                    {
                        'ecmness': {
                            '$exists': True
                        }
                    }
                ]
            },
            {
                'type': 'protein'
            }
        ]
    }):

        if ecm_protein['dataset'] != 'Swiss-Prot':
            continue

        protein_id = ecm_protein["id"]
        matrixdb_ecm = False
        if "ecm" in ecm_protein and ecm_protein["ecm"]:
            matrixdb_ecm = True

        matrisome_details= ''
        if "ecmness" in ecm_protein and ecm_protein["ecmness"]:
            if "category" in ecm_protein["ecmness"]["matrisome"]:
                matrisome_details = ecm_protein["ecmness"]["matrisome"]["category"]

            if "division" in ecm_protein["ecmness"]["matrisome"]:
                matrisome_details += f'|{ecm_protein["ecmness"]["matrisome"]["division"]}'

        go_terms = []
        if "go" in ecm_protein["annotations"]:
            for go in ecm_protein['annotations']['go']:
                if go in go_mapping:
                    go_terms.append(go_mapping[go])

        keywords = []
        if 'keywords' in ecm_protein['annotations']:
            for kw in ecm_protein['annotations']['keywords']:
                if type(kw) == dict:
                    kw = kw['id']
                if kw in kw_mapping:
                    keywords.append(kw_mapping[kw])

        if protein_id in interactions_counts:
            interaction_count = interactions_counts[protein_id]
        else:
            interaction_count = 0

        dataset = ecm_protein["dataset"]

        species_ncbi = ecm_protein["species"]["id"]

        go_terms = '|'.join(go_terms)
        keywords = '|'.join(keywords)
        line = f'{protein_id},{species_ncbi},{go_terms},{keywords},{dataset},{interaction_count}\n'
        lines.append({
            'protein_id': protein_id,
            'matrixdb_ecm': matrixdb_ecm,
            'matrisome': matrisome_details,
            'species_ncbi': species_ncbi,
            'go_terms': go_terms,
            'keywords': keywords,
            'dataset': dataset,
            'interaction_count': interaction_count
        })

    df = pd.DataFrame(lines)

    df['interaction_count'] = df['interaction_count'].astype(int)
    # Sort by interaction_count
    df = df.sort_values(by='interaction_count', ascending=False)
    df.to_csv('ecm_proteins.csv', index=False)

if __name__ == '__main__':
    generate_ecm_file()
    #generate_core_interaction_psimitab()
    #generate_all_interaction_psimitab()