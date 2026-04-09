import concurrent.futures
import logging
import time
from concurrent.futures import wait
import re
import traceback
from itertools import groupby

from src.matrixdb.model.builders.experiment_builder import ExperimentBuilder
from src.matrixdb.model.builders.interaction_builder import InteractionBuilder
from src.matrixdb.pipeline_manager.database_manager import DatabaseManager

biomolecule_cache = {}


def build_biomolecule_cache(target_connection):
    ecm_count = 0
    for biomolecule in target_connection["biomolecules"].find():

        if biomolecule["type"] == 'protein':
            if "ecm" in biomolecule and biomolecule["ecm"] is True:
                biomolecule_cache[biomolecule["id"]] = biomolecule["id"]
                ecm_count += 1
                continue

            if "ecmness" in biomolecule:
                biomolecule_cache[biomolecule["id"]] = biomolecule["id"]
                ecm_count += 1
                continue

        if biomolecule["type"] == 'gag' or biomolecule["type"] == 'smallmol' or biomolecule["type"] == "multimer"\
                or biomolecule["type"] == 'spep' or biomolecule["type"] == 'pfrag' or biomolecule['type'] == 'lipid':

            if biomolecule['id'] == 'LIP_1':
                print()
            ecm_count += 1
            biomolecule_cache[biomolecule["id"]] = biomolecule["id"]
            if biomolecule["type"] == 'spep':
                if "xrefs" in biomolecule and "ebi" in biomolecule["xrefs"]:
                    biomolecule_cache[biomolecule["xrefs"]["ebi"]] = biomolecule["id"]
                if "xrefs" in biomolecule and "chebi" in biomolecule["xrefs"]:
                    biomolecule_cache[biomolecule["xrefs"]["chebi"]] = biomolecule["id"]
            if "xrefs" in biomolecule:
                if biomolecule['type'] == 'lipid':
                    biomolecule_cache['CHEBI:'+biomolecule["xrefs"]["chebi"]] = biomolecule["id"]
                if "chebi" in biomolecule["xrefs"]:
                    biomolecule_cache[biomolecule["xrefs"]["chebi"]] = biomolecule["id"]
                if "complex_portal" in biomolecule["xrefs"]:
                    biomolecule_cache[biomolecule["xrefs"]["complex_portal"]] = biomolecule["id"]
                if "uniprot" in biomolecule["xrefs"]:
                    biomolecule_cache[biomolecule["xrefs"]["uniprot"]] = biomolecule["id"]


def process_pubmeds(pubmeds, source_connection, target_connection):
    try:
        # Get the distinct pubmeds and process interactions by each pubmed
        interations_to_load = list()
        experiments_to_load = list()

        for pubmed in pubmeds:
            irrelavant_publication = False

            interations_to_load_by_pubmed = list()
            experiments_to_load_by_pubmed = list()

            raw_interactions_by_pubmed = source_connection["matrixdbCoreInteractions"].find({
                        "Publication Identifier(s)": pubmed
            })

            relavant_interactions_by_intact_id = list()
            for interaction in raw_interactions_by_pubmed:

                pattern = re.compile(r'(uniprotkb|chebi|cpx|matrixdb|intact):([^|]+)', re.IGNORECASE)
                match = pattern.search(interaction["#ID(s) interactor A"])
                if match is None:
                    match = pattern.search(interaction["Alt. ID(s) interactor A"])
                    if match is None:
                        continue
                    interaction["#ID(s) interactor A"] = match.group(2).strip('"')
                    # Check for uniprot isoforms and possibly chains
                    if match.group(1) == 'uniprotkb':
                        if "-" in interaction["#ID(s) interactor A"]:
                            if "PRO" in interaction["#ID(s) interactor A"]:
                                # Chain or a fragment
                                # need to check if it a matrixdb pfrag
                                if interaction["#ID(s) interactor A"] in biomolecule_cache:
                                    interaction["#ID(s) interactor A"] = biomolecule_cache[
                                        interaction["#ID(s) interactor A"]]
                                else:
                                    print(f'PRO not in matrixdb {interaction["#ID(s) interactor A"]}')
                            else:
                                id = interaction["#ID(s) interactor A"].split('-')[0]
                                isoform = interaction["#ID(s) interactor A"].split('-')[1]
                                # interaction["#ID(s) interactor A"] = id
                                interaction["interactor_a_details"] = {
                                    "isoform": isoform
                                }

                    elif match.group(1) == 'chebi' or match.group(1) == 'cpx':
                        if interaction["#ID(s) interactor A"] in biomolecule_cache:
                            interaction["#ID(s) interactor A"] = biomolecule_cache[interaction["#ID(s) interactor A"]]
                        else:
                            part_a = interaction["#ID(s) interactor A"]
                            # print(f"Ignoring interaction {part_a} {interaction['intact_id']}")
                            #continue

                    elif match.group(1) == 'intact':
                        if interaction["#ID(s) interactor A"] in biomolecule_cache:
                            interaction["#ID(s) interactor A"] = biomolecule_cache[interaction["#ID(s) interactor A"]]
                        else:
                            # Almost a hack
                            pattern = re.compile(r'(matrixdb):([^|]+)', re.IGNORECASE)
                            match = pattern.search(interaction["Alt. ID(s) interactor A"])
                            if match is not None:
                                interaction["#ID(s) interactor A"] = match.group(2).strip('"')


                else:
                    interaction["#ID(s) interactor A"] = match.group(2).strip('"')
                    # Check for uniprot isoforms and possibly chains
                    if match.group(1) == 'uniprotkb':
                        if "-" in interaction["#ID(s) interactor A"]:
                            if "PRO" in interaction["#ID(s) interactor A"]:
                                # Chain or a fragment
                                # need to check if it a matrixdb pfrag
                                if interaction["#ID(s) interactor A"] in biomolecule_cache:
                                    interaction["#ID(s) interactor A"] = biomolecule_cache[
                                        interaction["#ID(s) interactor A"]]
                                else:
                                    print(f'PRO not in matrixdb {interaction["#ID(s) interactor A"]}')
                            else:
                                id = interaction["#ID(s) interactor A"].split('-')[0]
                                isoform = interaction["#ID(s) interactor A"].split('-')[1]
                                # interaction["#ID(s) interactor A"] = id
                                interaction["interactor_a_details"] = {
                                    "isoform": isoform
                                }

                    if match.group(1) == 'chebi' or match.group(1) == 'cpx':
                        if interaction["#ID(s) interactor A"] in biomolecule_cache:
                            interaction["#ID(s) interactor A"] = biomolecule_cache[interaction["#ID(s) interactor A"]]
                        else:
                            part_a = interaction["#ID(s) interactor A"]
                            # print(f"Ignoring interaction {part_a} {interaction['intact_id']}")
                            #continue

                    if match.group(1) == 'intact':
                        if interaction["#ID(s) interactor A"] in biomolecule_cache:
                            interaction["#ID(s) interactor A"] = biomolecule_cache[interaction["#ID(s) interactor A"]]
                        else:
                            # Almost a hack
                            pattern = re.compile(r'(matrixdb):([^|]+)', re.IGNORECASE)
                            match = pattern.search(interaction["#ID(s) interactor A"])
                            if match is not None:
                                interaction["#ID(s) interactor A"] = match.group(2).strip('"')

                pattern = re.compile(r'(uniprotkb|chebi|cpx|matrixdb|intact):([^|]+)', re.IGNORECASE)
                match = pattern.search(interaction["ID(s) interactor B"])
                if match is None:
                    match = pattern.search(interaction["Alt. ID(s) interactor B"])
                    if match is None:
                        continue
                    interaction["ID(s) interactor B"] = match.group(2).strip('"')

                    # Check for uniprot isoforms and possibly chains
                    if match.group(1) == 'uniprotkb':
                        if "-" in interaction["ID(s) interactor B"]:
                            if "PRO" in interaction["ID(s) interactor B"]:
                                if interaction["ID(s) interactor B"] in biomolecule_cache:
                                    interaction["ID(s) interactor B"] = biomolecule_cache[
                                        interaction["ID(s) interactor B"]]
                                else:
                                    print(f'PRO not in matrixdb {interaction["ID(s) interactor B"]}')
                            else:
                                id = interaction["ID(s) interactor B"].split('-')[0]
                                isoform = interaction["ID(s) interactor B"].split('-')[1]
                                # interaction["ID(s) interactor B"] = id
                                interaction["interactor_a_details"] = {
                                    "isoform": isoform
                                }

                    elif match.group(1) == 'chebi' or match.group(1) == 'cpx':
                        if interaction["ID(s) interactor B"] in biomolecule_cache:
                            interaction["ID(s) interactor B"] = biomolecule_cache[interaction["ID(s) interactor B"]]
                        else:
                            part_b = interaction["ID(s) interactor B"]
                            # print(f"Ignoring interaction {part_b} {interaction['intact_id']}")
                            #continue

                    elif match.group(1) == 'intact':
                        if interaction["ID(s) interactor B"] in biomolecule_cache:
                            interaction["ID(s) interactor B"] = biomolecule_cache[interaction["ID(s) interactor B"]]

                        else:
                            # Almost a hack
                            pattern = re.compile(r'(matrixdb):([^|]+)', re.IGNORECASE)
                            match = pattern.search(interaction["Alt. ID(s) interactor B"])
                            if match is not None:
                                interaction["ID(s) interactor B"] = match.group(2).strip('"')

                    if interaction["#ID(s) interactor A"] in biomolecule_cache or interaction[
                        "ID(s) interactor B"] in biomolecule_cache:
                        relavant_interactions_by_intact_id.append(interaction)
                    else:
                        print(f'Irrelavant {interaction["#ID(s) interactor A"]}  -- {interaction["ID(s) interactor B"]}  pubmed {pubmed}')
                        i = 1
                else:
                    interaction["ID(s) interactor B"] = match.group(2).strip('"')

                    # Check for uniprot isoforms and possibly chains
                    if match.group(1) == 'uniprotkb':
                        if "-" in interaction["ID(s) interactor B"]:
                            if "PRO" in interaction["ID(s) interactor B"]:
                                if interaction["ID(s) interactor B"] in biomolecule_cache:
                                    interaction["ID(s) interactor B"] = biomolecule_cache[
                                        interaction["ID(s) interactor B"]]
                                else:
                                    print(f'PRO not in matrixdb {interaction["ID(s) interactor B"]}')
                            else:
                                id = interaction["ID(s) interactor B"].split('-')[0]
                                isoform = interaction["ID(s) interactor B"].split('-')[1]
                                interaction["ID(s) interactor B"] = id
                                interaction["interactor_a_details"] = {
                                    "isoform": isoform
                                }

                    if match.group(1) == 'chebi' or match.group(1) == 'cpx':
                        if interaction["ID(s) interactor B"] in biomolecule_cache:
                            interaction["ID(s) interactor B"] = biomolecule_cache[interaction["ID(s) interactor B"]]
                        else:
                            part_b = interaction["ID(s) interactor B"]
                            # print(f"Ignoring interaction {part_b} {interaction['intact_id']}")
                            #continue

                    if match.group(1) == 'intact':
                        if interaction["ID(s) interactor B"] in biomolecule_cache:
                            interaction["ID(s) interactor B"] = biomolecule_cache[interaction["ID(s) interactor B"]]
                        else:
                            # Almost a hack
                            pattern = re.compile(r'(matrixdb):([^|]+)', re.IGNORECASE)
                            match = pattern.search(interaction["Alt. ID(s) interactor B"])
                            if match is not None:
                                interaction["ID(s) interactor B"] = match.group(2).strip('"')

                    if interaction["#ID(s) interactor A"] in biomolecule_cache or interaction[
                        "ID(s) interactor B"] in biomolecule_cache:
                        relavant_interactions_by_intact_id.append(interaction)
                    else:
                        print()

            print(f'To load {len(relavant_interactions_by_intact_id)}')


            #sorted_data = sorted(all_interactions_by_pubmed, key=lambda x: x['intact_id'])
            grouped_data = {key: list(group) for key, group in groupby(relavant_interactions_by_intact_id, key=lambda x: x['matrixdb_id'])}

            interactions_by_pubmed = dict()
            experiments_ids_by_pubmed = dict()

            # Check if at least one participant is in ECM
            for intact_id in grouped_data:
                interactions_to_process = grouped_data[intact_id]

                interactions_by_intact_id = dict()
                for interaction in interactions_to_process:
                    # ECM related interaction
                    interaction_builder = InteractionBuilder(intact_id=interaction["matrixdb_id"],
                                                             intact_interaction=interaction)
                    new_interaction = interaction_builder.build()

                    if new_interaction.get_id() not in interactions_by_pubmed:
                        interactions_by_pubmed[new_interaction.get_id()] = new_interaction
                    else:
                        new_interaction = interactions_by_pubmed[new_interaction.get_id()]
                    interations_to_load_by_pubmed.append(new_interaction)
                    interactions_by_intact_id[intact_id] = new_interaction

                    experiment_builder = ExperimentBuilder(intact_id=intact_id,
                                                           experiment_ids_by_pubmed=experiments_ids_by_pubmed,
                                                           intact_interaction_group=grouped_data[intact_id])
                    experiment = experiment_builder.build()
                    experiment_id = experiment.get_id()
                    if 'spoke expansion' in experiment.get_expansion_method():
                        new_interaction.add_spoke_expanded_from_experiment(experiment_id)
                    else:
                        new_interaction.add_binary_experiment(experiment_id)

                experiments_to_load_by_pubmed.append(experiment)

            interations_to_load.extend((i.to_json() for i in list(interactions_by_pubmed.values())))
            exps = dict()
            for e in experiments_to_load_by_pubmed:
                if e.get_id() not in exps:
                    exps[e.get_id()] = 1
                    experiments_to_load.append(e)

        all_int_interactions = set()
        if len(interations_to_load) > 0:
            for i in interations_to_load:
                if 'experiments' in i:
                    if 'direct' in i['experiments']:
                        if 'binary' in i['experiments']['direct']:
                            for b in i['experiments']['direct']['binary']:
                                all_int_interactions.add(b)
                        if 'spoke_expanded_from' in i['experiments']['direct']:
                            for b in i['experiments']['direct']['spoke_expanded_from']:
                                all_int_interactions.add(b)

                i['_meta'] = dict()
                i['_meta']['core'] = True
                i['_meta']['from_version'] = '3.5'
            target_connection["interactions"].insert_many(interations_to_load)

        if len(experiments_to_load) > 0:
            ser_exp_to_load = list()
            for e in experiments_to_load:
                e = e.to_json()
                e['_meta'] = dict()
                e['_meta']['core'] = True
                e['_meta']['from_version'] = '3.5'
                ser_exp_to_load.append(e)
            target_connection["experiments"].insert_many(ser_exp_to_load)

        return 1

    except Exception as e:
        print(e)
        traceback.print_exc()
        return -1


def execute(config, database_manager):
    start = time.time()
    pipeline_config = config["dependencies"]["intact"]
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

    print('Building biomolecule cache')
    build_biomolecule_cache(target_connection)
    print(f"Biomolecule cache built {len(biomolecule_cache)}")

    pubmed_identifiers = source_connection["matrixdbCoreInteractions"].distinct("Publication Identifier(s)", {'intact_id': '-'})
    pubmed_identifiers = list(filter(lambda p: not type(p) == float, pubmed_identifiers))
    process_pubmeds(pubmed_identifiers, source_connection, target_connection)


    print(f"Batches  completed")
    end = time.time() - start
    logging.info({
        'source': 'intact',
        'elapsed_time': end
    })


if __name__ == "__main__":


    database_manager = DatabaseManager()
    source = database_manager.get_connection(
        database_name='matrixdb-data-fabric',
        host='localhost:27018',
        port='27018'
    )
    target = database_manager.get_connection(
        database_name='matrixdb_4_0',
        host='localhost:27018',
        port='27018'
    )
    build_biomolecule_cache(target)
    #process_pubmeds(['23085623', '17991437'], 1, source, target)
    #process_pubmeds(['10022829'], 1, source, target)
    #process_pubmeds(['26752465'], 1, source, target)
    #process_pubmeds(['32296183'], 1, source, target)

    # with isoform
    #process_pubmeds(['22802125'], 1, source, target)

    #process_pubmeds(['28514442'],1, source, target)
    #process_pubmeds(['7287752'],1, source, target)
    process_pubmeds(['9431988'],1,source, target)

