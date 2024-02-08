import re

from src.matrixdb.model.experiment import Experiment


class ExperimentBuilder:

    def __init__(self, intact_id, experiment_ids_by_pubmed,intact_interaction_group):
        self.intact_id = intact_id
        self.experiment_ids_by_pubmed = experiment_ids_by_pubmed
        self.intact_interaction_group = intact_interaction_group

    def build(self):

        # Take the any (first) from the grouped interactions
        interaction = self.intact_interaction_group[0]
        # Experiment details
        split_by_pipe = interaction["Interaction identifier(s)"].split('|')
        intact_id = None
        imex_id = None

        # Iterate over the substrings after splitting by pipe
        for substring in split_by_pipe:
            # Split each substring based on the colon
            if not "intact" in substring and not "imex" in substring:
                continue

            key, value = substring.split(':')

            # Check the key and assign the value accordingly
            if key == 'intact':
                intact_id = value
            elif key == 'imex':
                imex_id = value

        pubmed = interaction["Publication Identifier(s)"]
        source = interaction["Source database(s)"]
        source_name = re.search(r'\(.*\)',source).group(0)
        source_name = source_name.replace('(', '')
        source_name = source_name.replace(')', '')
        expansion_method = interaction["Expansion method(s)"]
        host_organism = interaction["Host organism(s)"]
        #xrefs = self.intact_interaction["Interaction Xref(s)"]
        xrefs = {
            "intact": intact_id,
            "imex": imex_id
        }
        annotations = interaction["Interaction annotation(s)"]
        interaction_detection_method = interaction["Interaction detection method(s)"]
        interaction_parameters = interaction["Interaction parameter(s)"]
        interaction_type = interaction["Interaction type(s)"]

        creation_date = interaction["Creation date"]
        update_date = interaction["Update date"]

        participants = list()
        for intact_interaction in self.intact_interaction_group:
            participant_a = intact_interaction["#ID(s) interactor A"]
            participant_a_details = dict()
            if "interactor_a_details" in intact_interaction:
                participant_a_details = {
                    "isoform": intact_interaction["interactor_a_details"]["isoform"]
                }
            participant_a_annotations = intact_interaction["Annotation(s) interactor A"]
            participant_a_biological_role = intact_interaction["Biological role(s) interactor A"]
            participant_a_experimental_role = intact_interaction["Experimental role(s) interactor A"]
            participant_a_features = intact_interaction["Feature(s) interactor A"]
            participant_a_stoichiometry = intact_interaction["Stoichiometry(s) interactor A"]
            participant_a_identification_method = intact_interaction["Identification method participant A"]

            # Check if participant_a exists in participants
            element_exists = lambda participant_id : any(participant.get("id") == participant_id for participant in participants)
            if not element_exists(participant_a):
                participants.append({
                    "id": participant_a,
                    "deatails": participant_a_details,
                    "features": participant_a_features,
                    "annotations": participant_a_annotations,
                    "biological_role": participant_a_biological_role,
                    "experimental_role": participant_a_experimental_role,
                    "stoichiometry": participant_a_stoichiometry,
                    "identification_method": participant_a_identification_method
                })

            participant_b = intact_interaction["ID(s) interactor B"]
            participant_b_details = dict()
            if "interactor_b_details" in intact_interaction:
                participant_b_details = {
                    "isoform": intact_interaction["interactor_b_details"]["isoform"]
                }
            participant_b_biological_role = intact_interaction["Biological role(s) interactor B"]
            participant_b_annotations = intact_interaction["Annotation(s) interactor B"]
            participant_b_experimental_role = intact_interaction["Experimental role(s) interactor B"]
            participant_b_features = intact_interaction["Feature(s) interactor B"]
            participant_b_stoichiometry = intact_interaction["Stoichiometry(s) interactor B"]
            participant_b_identification_method = intact_interaction["Identification method participant B"]

            if not element_exists(participant_b):
                participants.append({
                    "id": participant_b,
                    "details": participant_b_details,
                    "features": participant_b_features,
                    "annotations": participant_b_annotations,
                    "biological_role": participant_b_biological_role,
                    "experimental_role": participant_b_experimental_role,
                    "stoichiometry": participant_b_stoichiometry,
                    "identification_method": participant_b_identification_method
                })

        # Extract participant ids to generate the id
        sorted_participants = sorted(participant["id"] for participant in participants)
        experiment_id_participant_part = None
        if len(sorted_participants) == 2 :
            experiment_id_participant_part = f"{sorted_participants[0]}__{sorted_participants[1]}_{pubmed}_{source_name}"
        elif len(sorted_participants) == 3 :
            experiment_id_participant_part = f"{sorted_participants[0]}__{sorted_participants[1]}__{sorted_participants[2]}_{pubmed}_{source_name}"
        elif len(sorted_participants) > 3:
            experiment_id_participant_part = f"{sorted_participants[0]}__{sorted_participants[1]}__...__{sorted_participants[-1]}_{pubmed}_{source_name}"
        elif len(sorted_participants) == 1:
            experiment_id_participant_part = f"{sorted_participants[0]}__{sorted_participants[0]}_{pubmed}_{source_name}"

        self.experiment_ids_by_pubmed[intact_id] = experiment_id_participant_part
        suffix = list(self.experiment_ids_by_pubmed.values()).count(experiment_id_participant_part)
        experiment_id = f'{experiment_id_participant_part}_{suffix}'

        return Experiment(
            experiment_id=experiment_id,
            participants=participants,
            pmid=pubmed,
            source=source,
            host_organisms=host_organism,
            interaction_detection_method=interaction_detection_method,
            interaction_type=interaction_type,
            annotations=annotations,
            xrefs=xrefs,
            expansion_method=expansion_method,
            parameters=interaction_parameters,
            creation_date=creation_date,
            update_date=update_date
        )