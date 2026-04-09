ecm_cache = dict()


def build_ecm_cache(target_connection):
    for ecm_protein in target_connection["biomolecules"].find({
        '$or': [
            {"ecm": True},
            {'ecmness': {'$exists': True}}
        ]
    }):
        ecm_cache[ecm_protein["id"]] = 1


def load_interaction_predictions(prediction_data_file, target_connection):
    ecm_ecm = 0
    ecm_non = 0
    non_non = 0
    only_predictions = 0
    with open(prediction_data_file) as ppi_file:
        line_number = 0
        predicted_interactions = list()
        for line in ppi_file:
            if line_number == 0:
                line_number += 1
                continue

            tabs = line.split('\t')

            print(line)
            ppi = tabs[0]
            interactor_1 = tabs[0]
            interactor_2 = tabs[1]
            publications = tabs[2]

            if interactor_1 in ecm_cache or interactor_2 in ecm_cache:
                ecm_non += 1

                sorted_participants = sorted([interactor_1, interactor_2])
                predicted_interactions.append({
                    "id": f'{sorted_participants[0]}__{sorted_participants[1]}',
                    "participants": [sorted_participants[0], sorted_participants[1]],
                    "prediction": True,
                    "prediction_studies": publications.split('|')
                })
            elif interactor_1 in ecm_cache and interactor_2 in ecm_cache:
                ecm_ecm += 1
            elif interactor_1 not in ecm_cache and interactor_2 not in ecm_cache:
                non_non += 1

        target_connection["interactions"].insert_many(predicted_interactions)

        print(f'Statistics ecm-ecm {ecm_ecm} ecm-non {ecm_non} non-non {non_non} only_predictions {only_predictions}')

def execute(config, database_manager):
    pipeline_config = config["pipelines"]["predictions"]

    dependency_config = config["dependencies"]["predictions"]
    prediction_raw_data = dependency_config['raw']
    target_host = dependency_config["target"]["host"]
    target_port = dependency_config["target"]["port"]
    target_database = dependency_config["target"]["database"]

    target_connection = database_manager.get_connection(
        database_name=target_database,
        host=target_host,
        port=target_port
    )

    build_ecm_cache(target_connection)
    load_interaction_predictions(prediction_raw_data, target_connection)