import traceback


class Interaction:

    def __init__(self, interaction_id, participants, pubmed, score):
        self.interaction_id = interaction_id
        self.participants = participants
        self.pubmed = pubmed
        self.score = score
        self.experiments = {
            "direct": {
                "binary": [],
                "spoke_expanded_from": []
            },
            "inferred": []
        }

    def get_id(self):
        return self.interaction_id

    def get_experiments(self):
        return self.experiments

    def add_binary_experiment(self, experiment_id):
        if experiment_id not in self.experiments["direct"]["binary"]:
            self.experiments["direct"]["binary"].append(experiment_id)

    def add_spoke_expanded_from_experiment(self, experiment_id):
        if experiment_id not in self.experiments["direct"]["spoke_expanded_from"]:
            self.experiments["direct"]["spoke_expanded_from"].append(experiment_id)

    def add_inferred_experiment(self, experiment_id):
        self.experiments["inferred"].append(experiment_id)

    def to_json(self):
        try:
            return {
                "id": self.interaction_id,
                "experiments": self.experiments,
                "participants": self.participants,
                "score": self.score
            }
        except:
            traceback.print_exc()

