import time
import logging

from src.matrixdb.utils.biomolecule_cache_provider import BiomoleculeCacheProvider


def set_pfrag_ecmness(target_connection, biomolecule_cache_provider):
    for pfrag in target_connection["biomolecules"].find({
        "type": "pfrag"
    }):
        if type(pfrag["relations"]["belongs_to"]) is list:
            for component in pfrag["relations"]["belongs_to"]:
                if biomolecule_cache_provider.is_ecm_biomolecule(component):
                    ecm_pfrag = True
                    break
        else:
            if biomolecule_cache_provider.is_ecm_biomolecule(pfrag["relations"]["belongs_to"]):
                ecm_pfrag = True

        if ecm_pfrag is not None and ecm_pfrag:
            pfrag["ecm"] = True
            target_connection["biomolecules"].update(
                {'id': pfrag['id']},
                {'$set': {'ecm': True}}
            )
            #print(pfrag["id"])
        else:
            print(f"No ecm pfrag {pfrag['id']}")


def set_multimer_ecmness(target_connection, biomolecule_cache_provider):
    for multimer in target_connection["biomolecules"].find({
        "type": "multimer"
    }):
        if "molecular_details" not in multimer:
            print(f"{multimer['id']}: no molecular details")
            continue

        if "stochiometry" not in multimer["molecular_details"]:
            print(f"{multimer['id']}: no stochiometry")
            continue

        for protein_component in multimer["molecular_details"]["stochiometry"]:
            protein = protein_component["id"]
            if biomolecule_cache_provider.is_ecm_biomolecule(protein):
                ecm_multimer = True
                break

        if ecm_multimer is not None and ecm_multimer:
            #print(multimer['id'])
            target_connection["biomolecules"].update(
                {'id': multimer['id']},
                {'$set': {'ecm': True}}
            )
        else:
            print(f"No ecm multimer {multimer['id']}")


def update_pdb(source_connection, target_connection):
    reviewed_protein_ids = [b["id"] for b in target_connection["biomolecules"].find({
        "dataset": 'Swiss-Prot'
    })]
    uniprot_protiens = list(source_connection["uniprotEntries"].find({
        "accession.text": {
            '$in': reviewed_protein_ids
        }
    }))

    for uniprot_protien in uniprot_protiens:
        pdb_details = list()
        for pdb in list(filter(lambda c: c["type"] == "PDB", uniprot_protien["dbReference"])):
            if "property" in pdb:
                properties = pdb["property"]

            pdb_details.append({
                "id": pdb["id"],
                "properties": properties,
            })

        if type(uniprot_protien["accession"]) == dict:
            uniprot_to_update = uniprot_protien["accession"]["text"]

        if type(uniprot_protien["accession"]) == list:
            uniprot_to_update = uniprot_protien["accession"][0]["text"]

        print(f'Updating {uniprot_to_update}')
        target_connection["biomolecules"].update_one(
            {
                "id": uniprot_to_update
            },
            {
                "$set": {
                    "molecular_details.pdb": pdb_details
                }
            })


def execute(config, database_manager):
    pipeline_config = config["pipelines"]["biomolecule_post_processing"]

    target_host = pipeline_config["target"]["host"]
    target_port = pipeline_config["target"]["port"]
    target_database = pipeline_config["target"]["database"]

    source_host = pipeline_config["source"]["host"]
    source_port = pipeline_config["source"]["port"]
    source_database = pipeline_config["source"]["database"]

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

    biomolecule_cache_provider = BiomoleculeCacheProvider(target_connection)
    update_pdb(source_connection, target_connection)
    #set_multimer_ecmness(target_connection, biomolecule_cache_provider)
    #set_pfrag_ecmness(target_connection, biomolecule_cache_provider)
