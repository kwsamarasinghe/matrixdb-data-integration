from pymongo import MongoClient

main_connection = MongoClient('mongodb://localhost:27018')['matrixdb_4_0']

with open('human_murine_proteins_interaction_counts', 'w') as output_file:
    output_file.write(f'UniProtAccession,Species,ExperimentalCount,PredictedCount')

    for h_protein in main_connection['biomolecules'].find({
        '$and': [
            {'species.id': '9606'},
            {'type': 'protein' }
        ]
    }):
        predicted_count = 0
        exp_count = 0
        for int in main_connection['interactions'].find({
            'participants': h_protein['id'],
        }):
            if 'prediction' in int:
                predicted_count += 1
            else:
                exp_count += 1

        output_file.write(f'{h_protein["id"]},Homo sapiens,{exp_count},{predicted_count}\n')

    for h_protein in main_connection['biomolecules'].find({
        '$and': [
            {'species.id': '10090'},
            {'type': 'protein'}
        ]
    }):
        predicted_count = 0
        exp_count = 0
        for int in main_connection['interactions'].find({
            'participants': h_protein['id'],
        }):
            if 'prediction' in int:
                predicted_count += 1
            else:
                exp_count += 1

        output_file.write(f'{h_protein["id"]},Mus musculus,{exp_count},{predicted_count}\n')