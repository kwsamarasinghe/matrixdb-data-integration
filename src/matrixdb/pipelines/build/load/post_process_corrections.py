from src.matrixdb.pipeline_manager.connection_provider import get_connection

target_connection = get_connection("mongodb://localhost:27018/", "matrixdb_4_0")

# need to correct the chebi xref of GAG_59
# GAG_60
# GAG_61
# GAG_62

gags_to_change = [
    {
        'gag_id': 'GAG_59',
        'chebi_id': 'CHEBI:194333',
    },
    {
        'gag_id': 'GAG_60',
        'chebi_id': 'CHEBI:194334'
    },
    {
        'gag_id': 'GAG_61',
        'chebi_id': 'CHEBI:194335'
    },
    {
        'gag_id': 'GAG_62',
        'chebi_id': 'CHEBI:60924'
    }
]

for gag in gags_to_change:
    target_connection['biomolecules'].update_one(
        {
            "id": gag['gag_id']
        },
        {
            '$set': {
                'xrefs': {
                    'chebi': gag['chebi_id'],
                }
            }
        }
    )