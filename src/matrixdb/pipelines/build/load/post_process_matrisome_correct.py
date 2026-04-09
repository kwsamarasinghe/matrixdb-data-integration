from src.matrixdb.pipeline_manager.connection_provider import get_connection

source_connection = get_connection("mongodb://localhost:27018/", "matrixdb-data-fabric")
target_connection = get_connection("mongodb://localhost:27018/", "matrixdb_4_0")

old_m_list = list(source_connection['matrisomeEntries'].find())
new_m_list = list(source_connection['matrisomeEntries2'].find())

#old_m_set = set(acc["accession"] for acc in old_m_list)
#new_m_set = set(acc["accession"] for acc in new_m_list)
#to_be_removed = old_m_set.difference(new_m_set)
#print()

for acc in to_be_removed:
    target_connection['biomolecules'].update_one(
        {
            "id": acc
        },
        {
            '$unset': {
                'ecmness': ''
            }
        }
    )