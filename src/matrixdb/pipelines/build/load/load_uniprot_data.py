'''
Loads the uniprot and trembl entries to pre-prod filtering according to the ECMness
ECMness is defined as follows

UniProtKB keywords
Basement membrane    KW-0084
Extracellular matrix     KW-0272

GO term Cellular Component
Extracellular matrix     GO:0031012
Basement membrane  GO:0005604
Extracellular space       GO:0005615
Extracellular region     GO:0005576

'''
from src.matrixdb.model.protein_transformer import convert_trembl, convert_uniprot


def load_trembl(source, target):
    trembl_entries = source["tremblEntries"].find({
        "$or": [
            {
                "keywords.id": {
                    "$in": [
                            "KW-0084",
                            "KW-0272"
                    ]
                }
            },
            {
                "uniProtKBCrossReferences.id": {
                    "$in": [
                        "GO:0031012",
                        "GO:0005604",
                        "GO:0005615",
                        "GO:0005576"
                    ]
                }
            }
        ]
    })

    trembl_count = 0
    trembls_to_load = list()
    for trembl_entry in trembl_entries:
        converted_trembl = convert_trembl(trembl_entry)
        converted_trembl["ecm"] = True
        trembls_to_load.append(converted_trembl)

        if len(trembls_to_load) > 10000:
            print("Writing 10000 trembl entries")
            target["biomolecules"].insert_many(trembls_to_load)
            del trembls_to_load
            trembls_to_load = list()

        trembl_count += 1

    print(f"Writing {len(trembls_to_load)} trembls")
    target["biomolecules"].insert_many(trembls_to_load)


def load_uniprot(source, target):
    # Retrieve all uniprot entries according to ECM
    uniprot_entries = source["uniprotEntries"].find({
        "$or": [
            {
                "keyword.id": {
                    "$in": [
                        "KW-0084",
                        "KW-0272"
                    ]
                }
            },
            {
                "dbReference.id": {
                    "$in": [
                        "GO:0031012",
                        "GO:0005604",
                        "GO:0005615",
                        "GO:0005576"
                    ]
                }
            }
        ]
    })

    uniprot_count = 0
    uniprots_to_load = list()

    for uniprot_entry in uniprot_entries:
        converted_uniprot = convert_uniprot(uniprot_entry)
        converted_uniprot["ecm"] = True
        uniprots_to_load.append(converted_uniprot)

        if len(uniprots_to_load) > 4000:
            print(f"Writing {len(uniprots_to_load)} uniprot entries")
            target["biomolecules"].insert_many(uniprots_to_load)
            del uniprots_to_load
            uniprots_to_load = list()

        uniprot_count += 1

    print(f"Writing {len(uniprots_to_load)} entries")
    target["biomolecules"].insert_many(uniprots_to_load)
    print(f"Uniprot {uniprot_count}")


def execute(config, database_manager):
    pipeline_config = config["dependencies"]["uniprot"]
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

    load_uniprot(source_connection, target_connection)
    load_trembl(source_connection, target_connection)
