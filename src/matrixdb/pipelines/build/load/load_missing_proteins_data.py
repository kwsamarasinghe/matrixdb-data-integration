def load_missing_proteins(source, target):
    missing_biomolecules = list()

    for biomolecule in missing_biomolecules:
        # Check if a core biomolecule
        if 'GAG' in biomolecule or 'MULT' in biomolecule or 'PFRAG' in biomolecule\
                or 'CAT_' in biomolecule or 'SPEP_' in biomolecule or 'LIP_' in biomolecule:
            # Cannot do anything here
            print(f"missing {biomolecule}")
        else:
            # Should load from uniprot or trembl
            for found_protein in  list(source["uniprotEntries"].find({
                "accession.text": biomolecule
            })):
                print()


def execute(config, database_manager):
    pipeline_config = config["dependencies"]["meta"]

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