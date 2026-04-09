import concurrent
from concurrent.futures import wait
import time
import logging


def merge_interactions(interactions):
    new_interaction = {
        'id': '',
        'participants': [],
        'experiments': {
            'direct': {
                'binary': [],
                'spoke_expanded_from': []
            }
        },
        'prediction_models': [],
        'score': 0
    }

    id = ''
    participants = set()
    direct = []
    spoke_expanded_from = []
    prediction_models = []
    score = set()

    for interaction in interactions:
        id = interaction['id']
        for p in interaction['participants']:
            participants.add(p)

        if 'experiments' in interaction:
            if 'direct' in interaction['experiments']:
                if 'binary' in interaction['experiments']['direct']:
                    direct.extend(interaction['experiments']['direct']['binary'])

                if 'spoke_expanded_from' in interaction['experiments']['direct']:
                    spoke_expanded_from.extend(interaction['experiments']['direct']['spoke_expanded_from'])

        if 'prediction_models' in interaction:
            prediction_models.extend(interaction['prediction_models'])

        if 'score' in interaction:
            score.add(interaction['score'])

    new_interaction['id'] = id
    new_interaction['participants'] = list(participants)
    if len(direct) > 0:
        new_interaction['experiments']['direct']['binary'] = direct

    if len(spoke_expanded_from) > 0:
        new_interaction['experiments']['direct']['spoke_expanded_from'] = spoke_expanded_from

    if len(prediction_models) > 0:
        print(f'predicted {interaction}')
        new_interaction['prediction_models'] = prediction_models

    if len(score) != 1:
        new_interaction['score'] = score.pop()

    return new_interaction


def post_process_batch(duplicated_interactions, batch_no, source_connection):
    print(f'Processing batch {batch_no}')
    merged_interactions = list()
    interaction_ids = list()
    for interaction in duplicated_interactions:
        interaction_id = interaction['_id']
        interaction_ids.append(interaction_id)
        interactions = list(source_connection['interactions'].find({
            'id': interaction_id
        }))
        new_interaction = merge_interactions(interactions)
        merged_interactions.append(new_interaction)

    print(f'Deleting {len(interaction_ids)} interactions')
    source_connection['interactions'].delete_many({
        'id': {
            '$in': interaction_ids
        }
    })
    print(f'Inserting {len(merged_interactions)} interactions')
    source_connection['interactions'].insert_many(merged_interactions)

    print(f'Completed batch {batch_no}')


def post_process_interactions(source_connection, target_connection):
    duplicated_interactions = list(source_connection['interactions'].aggregate([
        {
            '$group': {
                '_id': "$id",
                'count': {'$sum': 1}
            }
        },
        {
            '$match': {
                'count': {'$gt': 1}
            }
        }
    ]))

    # Number of threads (adjust as needed)
    start = time.time()
    num_threads = 6

    # Function to process pubmeds using threads
    # Split the list into chunks for parallel processing
    chunk_size = len(duplicated_interactions) // num_threads
    pubmed_chunks = [duplicated_interactions[i:i + chunk_size] for i in range(0, len(duplicated_interactions), chunk_size)]

    # Execute in parallel with batch numbers
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = list()
        for batch_number, duplicated_interactions in enumerate(pubmed_chunks, start=1):
            futures.append(
                executor.submit(post_process_batch, duplicated_interactions, batch_number,
                                source_connection))
        wait(futures)

    print(f"Batches  completed")
    end = time.time() - start
    logging.info({
        'step': 'intacteraction_post_processing',
        'elapsed_time': end
    })


def verify_interactions(target_connection):
    # Set valid to false for EBI- interactor related interactions
    target_connection['interactions'].update_many(
        {
            'participants': {
                '$regex': 'EBI-'
            }
        },
        {
            '$set': {
                'valid': False
            }
        }
    )

    # Set the valid to false for CHEBI: interactor related interactions
    target_connection['interactions'].update_many(
        {
            'participants': {
                '$regex': 'CHEBI:'
            }
        },
        {
            '$set': {
                'valid': False
            }
        }
    )


def execute(config, database_manager):
    pipeline_config = config["pipelines"]["intact_post_processing"]
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

    post_process_interactions(source_connection, target_connection)

    # Invalidate interactions with biomolecules which are not important
    #verify_interactions(target_connection)