import logging

from src.matrixdb.model.protein_transformer import convert_trembl, convert_uniprot
from src.matrixdb.utils.protein_entry_status_provider import ProteinStatusProvider


def load_matrisome_proteins(source, target):
    loaded_not_in_ecm = 0
    already_in_ecm = 0
    to_load = list()
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

        if found_entry is not None:
            # Updates the entry with matrisome categories
            category = matrisome_entry["category"]
            division = matrisome_entry["division"]

            target["biomolecules"].update_one(
                {
                    "id": matrisome_accession
                },
                {
                    "$set": {
                        "ecmness": {
                            "matrisome": {
                                "category": category,
                                "division": division
                            }
                        }
                    }
                }
            )

        else:
            # Have to find in uniprot or trembl
            found_entry = source["uniprotEntries"].find_one({
                "accession.text": matrisome_entry["accession"]
            })

            if found_entry is not None:
                # Have to check if the entry is merged
                if type(found_entry["accession"]) is list:
                    primary_accession = found_entry["accession"][0]["text"]
                    if matrisome_entry["accession"] != primary_accession:
                        # Merged
                        print(f"Merged the entry {matrisome_entry} to {primary_accession}")
                        continue

                # Have to load the entry
                converted_uniprot = convert_uniprot(found_entry)
                category = matrisome_entry["category"]
                division = matrisome_entry["division"]
                converted_uniprot["ecmness"] = {
                    "matrisome": {
                        "category": category,
                        "division": division
                    }
                }
                entry_id = converted_uniprot["id"]
                print(f"Inserting {entry_id}")
                #target["biomolecules"].insert_one(converted_uniprot)
                to_load.append(converted_uniprot)
                loaded_not_in_ecm += 1

            else:
                # Try trembl
                found_entry = source["tremblEntries"].find_one({
                    "primaryAccession": matrisome_entry["accession"]
                })
                if found_entry is None:
                    print(f"Obsolete entry {matrisome_entry}")
                    continue
                converted_entry = convert_trembl(found_entry)
                entry_id = converted_entry["id"]
                converted_entry["ecmness"] = {
                    "matrisome": {
                        "category": category,
                        "division": division
                    }
                }
                print(f"Inserting {entry_id}")
                #target["biomolecules"].insert_one(converted_entry)
                to_load.append(converted_entry)
                loaded_not_in_ecm += 1

    target["biomolecules"].insert_many(to_load)

    logging.info({
        'source': 'matrisome',
        'read_from_source': len(disctint_matrisome_accessions),
        'load_to_target': loaded_not_in_ecm,
        'already_in_ecm': already_in_ecm
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