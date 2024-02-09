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
    proteins_to_load = list()
    for biomolecule in list(missing_participants):
        # Check if a core biomolecule
        if 'GAG' in biomolecule or 'MULT' in biomolecule or 'PFRAG' in biomolecule\
                or 'CAT_' in biomolecule or 'SPEP_' in biomolecule or 'LIP_' in biomolecule:
            # Cannot do anything here
            print(f"missing {biomolecule}")
        else:
            protein_status_provide = ProteinStatusProvider(source)
            protein_status_list = protein_status_provide.get_protein_entry_status(missing_participants)

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

    load_missing_proteins(source_connection, target_connection)