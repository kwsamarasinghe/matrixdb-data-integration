import pandas as pd
import json
from pymongo import MongoClient

CONFIG_FILE = "./conf/config.json"
database = "matrixdb-data-fabric"


def get_db_connection():
    CONNECTION_STRING = "mongodb://localhost:27018/"
    return MongoClient(CONNECTION_STRING)[database]


with open(CONFIG_FILE) as config:
    app_config = json.load(config)
    matrixdb_core_file_location = app_config["matrixdb_core"]["associations"]

psmitab_intact_full_file = matrixdb_core_file_location + "matrixdb_CORE.tab.gz"
df = pd.read_csv(psmitab_intact_full_file, sep='\t', compression='gzip')

# Extract taxids from 'Taxid interactor A' column using regex
df['Taxid interactor A'] = df['Taxid interactor A'].str.extract(r'taxid:(-?\d+)')
df['Taxid interactor B'] = df['Taxid interactor B'].str.extract(r'taxid:(-?\d+)')
df["Publication Identifier(s)"] = df["Publication Identifier(s)"].str.extract(r'pubmed:(\d+)')

# Convert the 'Taxid interactor A' column to strings for comparison
df['Taxid interactor A'] = df['Taxid interactor A'].astype(str)
df['Taxid interactor B'] = df['Taxid interactor B'].astype(str)


# Define a function to convert each row to JSON
def row_to_json(row):
    interaction_ids = row['Interaction identifier(s)'].split('|')
    main_id = interaction_ids[0].split(':')[1]
    other_ids = { key: value for key, value in (id_string.split(':') for id_string in interaction_ids[1:]) }
    json_data = {
        'id': main_id,
        'other_ids': other_ids
    }

    for column in df.columns:
        json_data[column] = row[column]
    return json_data

# Apply the function to each row and convert the result to a list of dictionaries
json_documents = df.apply(row_to_json, axis=1).tolist()

db_connection = get_db_connection()
db_connection["matrixdbCoreInteractions"].insert_many(json_documents)
print()