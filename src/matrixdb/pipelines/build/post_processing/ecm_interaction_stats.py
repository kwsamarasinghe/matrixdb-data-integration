from pymongo import MongoClient
import matplotlib.pyplot as plt

client = MongoClient("mongodb://localhost:27018")
db = client["matrixdb_4_0"]
interactions = db["interactions"]

pipeline = [
    {
        "$project": {
            "id": 1,
            "prediction": 1,
            "experiments": 1,
            "participant1": { "$arrayElemAt": ["$participants", 0] },
            "participant2": { "$arrayElemAt": ["$participants", 1] }
        }
    },
    {
        "$lookup": {
            "from": "biomolecules",
            "localField": "participant1",
            "foreignField": "id",
            "as": "participant1_info"
        }
    },
    {
        "$lookup": {
            "from": "biomolecules",
            "localField": "participant2",
            "foreignField": "id",
            "as": "participant2_info"
        }
    },
    {
        "$addFields": {
            "participant1_info": {
                "$first": {
                    "$filter": {
                        "input": "$participant1_info",
                        "as": "bm",
                        "cond": { "$eq": ["$$bm.species.id", "9606"] }
                    }
                }
            },
            "participant2_info": {
                "$first": {
                    "$filter": {
                        "input": "$participant2_info",
                        "as": "bm",
                        "cond": { "$eq": ["$$bm.species.id", "9606"] }
                    }
                }
            }
        }
    },
    {
        "$match": {
            "participant1_info": { "$ne": None },
            "participant2_info": { "$ne": None }
        }
    },
    {
        "$project": {
            "id": 1,
            "prediction": 1,
            "experiments": 1,
            "participant1_info": {
                "id": "$participant1_info.id",
                "species": "$participant1_info.species.id",
                "ecm": "$participant1_info.ecm",
                "ecmness": "$participant1_info.ecmness"
            },
            "participant2_info": {
                "id": "$participant2_info.id",
                "species": "$participant2_info.species.id",
                "ecm": "$participant2_info.ecm",
                "ecmness": "$participant2_info.ecmness"
            }
        }
    }
]


results = interactions.aggregate(pipeline)

matrixdb_ecm_predicted = 0
matrixdb_ecm_experimental = 0
matrisome_ecm_predicted = 0
matrisome_ecm_experimental = 0

with open("interaction_summary.csv", mode="w", newline="", encoding="utf-8") as file:
    # Write CSV header line
    header = [
        "interaction_id",
        "participant_1",
        "participant_1_species",
        "participant_1_ecm",
        "participant_1_matrisome_category",
        "participant_2",
        "participant_2_species",
        "participant_2_ecm",
        "participant_2_matrisome_category",
        "is_predicted",
        "is_experimental"
    ]
    file.write(",".join(header) + "\n")

    for doc in results:
        participant_1 = doc.get("participant1_info", {})
        participant_2 = doc.get("participant2_info", {})

        # Skip non-human interactions
        if participant_1.get("species") != '9606' or participant_2.get("species") != '9606':
            print("Non-human interaction skipped")
            continue

        is_predicted = bool(doc.get("prediction"))
        is_experimental = bool(doc.get("experiments"))

        ecm_present = participant_1.get("ecm") is True or participant_2.get("ecm") is True
        matrisome_present = "ecmness" in participant_1 or "ecmness" in participant_2

        # Update counts
        if ecm_present:
            if is_predicted:
                matrixdb_ecm_predicted += 1
            if is_experimental:
                matrixdb_ecm_experimental += 1

        if matrisome_present:
            if is_predicted:
                matrisome_ecm_predicted += 1
            if is_experimental:
                matrisome_ecm_experimental += 1

        # Prepare CSV row data (convert all to strings)
        # Should sort the participant by id
        p1_id = participant_1.get("id", "")
        p2_id = participant_2.get("id", "")
        if p1_id > p2_id:
            # Should swap p1 and p2
            tmp = participant_2
            participant_2 = participant_1
            participant_1 = tmp

        row = [
            str(doc.get("id", "")),
            str(participant_1.get("id", "")),
            str(participant_1.get("species", "")),
            str(participant_1.get("ecm", False)),
            str(participant_1.get("ecmness", "")),
            str(participant_2.get("id", "")),
            str(participant_2.get("species", "")),
            str(participant_2.get("ecm", False)),
            str(participant_2.get("ecmness", "")),
            str(is_predicted),
            str(is_experimental)
        ]

        # Escape any commas by wrapping values in quotes if needed
        escaped_row = [f'"{val}"' if "," in val else val for val in row]

        # Write row line
        file.write(",".join(escaped_row) + "\n")


print("MatrixDB ECM interactions (predicted):", matrixdb_ecm_predicted)
print("MatrixDB ECM interactions (experimental):", matrixdb_ecm_experimental)
print("Matrisome ECM interactions (predicted):", matrisome_ecm_predicted)
print("Matrisome ECM interactions (experimental):", matrisome_ecm_experimental)


categories = [
    "MatrixDB ECM (Predicted)",
    "MatrixDB ECM (Experimental)",
    "Matrisome ECM (Predicted)",
    "Matrisome ECM (Experimental)"
]

counts = [
    matrixdb_ecm_predicted,
    matrixdb_ecm_experimental,
    matrisome_ecm_predicted,
    matrisome_ecm_experimental
]

plt.figure(figsize=(10, 6))
bars = plt.bar(categories, counts, color=["red", "black", "red", "black"])
plt.ylabel("Interaction Count")
plt.title("ECM Interaction Classification")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()

# Add counts on top of bars
for bar, count in zip(bars, counts):
    plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), str(count),
             ha='center', va='bottom')

plt.savefig("ecm_interaction_classification.png", dpi=300)

plt.show()

