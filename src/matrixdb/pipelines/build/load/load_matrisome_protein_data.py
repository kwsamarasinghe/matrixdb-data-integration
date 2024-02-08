import logging

from src.matrixdb.model.protein_transformer import convert_trembl, convert_uniprot
from src.matrixdb.utils.protein_entry_status_provider import ProteinStatusProvider


def load_matrisome_proteins(source, target):
    protein_status_provide = ProteinStatusProvider(source)
    loaded_not_in_ecm = 0
    already_in_ecm = 0
    obsolete = 0
    disctint_matrisome_accessions = list(source["matrisomeEntries"].distinct("accession"))
    for matrisome_entry in source["matrisomeEntries"].find({
        "accession": {
            "$in": disctint_matrisome_accessions
        }
    }):
        matrisome_accession = matrisome_entry["accession"]
        found_entry = target["biomolecules"].find_one({
            "id": matrisome_entry["accession"]
        })

        category = matrisome_entry["category"]
        division = matrisome_entry["division"]
        if found_entry is not None:
            # Updates the entry with matrisome categories
            target["biomolecules"].update_one(
                {
                    "id": matrisome_accession
                },
                {
                    "$set": {
                        "matrisome": {
                            "category": category,
                            "division": division
                        }
                    }
                })
            already_in_ecm += 1

        else:
            protein_status = protein_status_provide.get_protein_entry_status(matrisome_accession)

            if 'obsolete' in protein_status:
                # Create an obsolete biomolecule node
                biomolecule = {
                    'id': protein_status['accession'],
                    'obsolete': True,
                    'type': 'protein'
                }
                if 'primaryAccession' in protein_status:
                    biomolecule['primaryAccession'] = protein_status['primaryAccession']
                obsolete += 1
            else:
                if 'trembl' in protein_status:
                    trembl_entry = protein_status['entry']
                    biomolecule = convert_trembl(trembl_entry)
                    biomolecule['matrisome'] = {
                            "category": category,
                            "division": division
                    }

                if 'uniprot' in protein_status:
                    uniprot_entry = protein_status['entry']
                    biomolecule = convert_uniprot(uniprot_entry)
                    biomolecule['matrisome'] = {
                            "category": category,
                            "division": division
                    }
                loaded_not_in_ecm += 1

            target['biomolecules'].insert_one(biomolecule)

    logging.info({
        'source': 'matrisome',
        'read_from_source': len(disctint_matrisome_accessions),
        'load_to_target': loaded_not_in_ecm,
        'already_in_ecm': already_in_ecm,
        'obsolete': obsolete,
    })


def execute(config, database_manager):
    pipeline_config = config["dependencies"]["matrisome_proteins"]
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

    load_matrisome_proteins(source_connection, target_connection)