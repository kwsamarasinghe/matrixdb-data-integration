import logging

from src.matrixdb.model.protein_transformer import convert_trembl, convert_uniprot
from src.matrixdb.utils.protein_entry_status_provider import ProteinStatusProvider


def load_missing_proteins(source, target):
    all_participants = set()

    # Existing biomolecules
    existing_biomolecules = set(biomolecule["id"] for biomolecule in target["biomolecules"].find())

    # Extract the missing proteins
    for interaction in list(target["interactions"].find()):
        for participant in interaction["participants"]:
            all_participants.add(participant)

    missing_participants = all_participants.difference(existing_biomolecules)
    logging.info(f"Missing participants: {len(missing_participants)}")

    missing_chebis = set(filter(lambda p: 'CHEBI' in p, missing_participants))
    logging.info(f"Missing chebi: {len(missing_chebis)}")

    missing_ebi = set(filter(lambda p: 'EBI-' in p, missing_participants))
    logging.info(f"Missing ebi: {len(missing_ebi)}")

    missing_proteins = set(missing_participants).difference(missing_chebis).difference(missing_ebi)
    logging.info(f"Missing proteins: {len(missing_proteins)}")

    # Extract the missing protein names considering isoform notation as well
    final_missing_proteins = set()
    for protein in missing_proteins:
        if '-' in protein:
            accession = protein.split('-')[0]
        else:
            accession = protein

        if accession not in existing_biomolecules:
            final_missing_proteins.add(accession)

    proteins_to_load = list()
    protein_status_provide = ProteinStatusProvider(source)
    protein_status_list = protein_status_provide.get_protein_entry_status(list(final_missing_proteins))

    for protein_status in protein_status_list:
        if 'obsolete' in protein_status:
            # Create an obsolete biomolecule node
            biomolecule = {
                'id': protein_status['accession'],
                'obsolete': True,
                'type': 'protein'
            }
            if 'primaryAccession' in protein_status:
                biomolecule['refer_to'] = protein_status['primaryAccession']
        else:
            if 'trembl' in protein_status:
                trembl_entry = protein_status['entry']
                biomolecule = convert_trembl(trembl_entry)

            if 'uniprot' in protein_status:
                uniprot_entry = protein_status['entry']
                biomolecule = convert_uniprot(uniprot_entry)
        proteins_to_load.append(biomolecule)

    logging.info({
        "source": "protein",
        "count": len(proteins_to_load)
    })
    target["biomolecules"].insert_many(proteins_to_load)


def remove_duplicates(source, target):
    # Check for duplicates
    duplicate_biomolecules = list()
    duplicate_ids = set()
    for biomolecule_id in list(target["biomolecules"].aggregate([
        {
            '$group': {
                '_id': '$id',
                'count': {
                    '$sum': 1
                }
            }
        },
        {
            '$match': {
                'count': {
                    '$gt': 1
                }
            }
        }
    ])):
        if biomolecule_id['_id'] not in duplicate_ids:
            duplicate_biomolecule = target["biomolecules"].find_one({
                'id': biomolecule_id['_id']
            })
            del duplicate_biomolecule["_id"]
            duplicate_biomolecules.append(duplicate_biomolecule)
            duplicate_ids.add(biomolecule_id['_id'])

    target["biomolecules"].delete_many({'id': {'$in': list(b['id'] for b in duplicate_biomolecules)}})
    target["biomolecules"].insert_many(duplicate_biomolecules)


def execute(config, database_manager):
    pipeline_config = config["pipelines"]["missing_proteins"]

    source_host = pipeline_config["source"]["host"]
    source_port = pipeline_config["source"]["port"]
    source_database = pipeline_config["source"]["database"]

    target_host = pipeline_config["target"]["host"]
    target_port = pipeline_config["target"]["port"]
    target_database = pipeline_config["target"]["database"]

    source_connection = database_manager.get_connection(
        database_name=source_database,
        host=source_host,
        port=source_port
    )

    target_connection = database_manager.get_connection(
        database_name=target_database,
        host=target_host,
        port=target_port
    )

    # Load missing proteins
    load_missing_proteins(source_connection, target_connection)

    # Remove duplicates
    #remove_duplicates(source_connection, target_connection)