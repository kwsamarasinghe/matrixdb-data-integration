import re

from src.matrixdb.model.interaction import Interaction


class InteractionBuilder:

    def __init__(self, intact_id, intact_interaction):
        self.intact_id = intact_id
        self.intact_interaction = intact_interaction

    def build(self):

        participant_a = self.intact_interaction["#ID(s) interactor A"]
        participant_b = self.intact_interaction["ID(s) interactor B"]
        score = self.intact_interaction["Confidence value(s)"]
        if score is not None:
            pattern = r'intact-miscore:(\d+(\.\d+)?)'

            match = re.search(pattern, score)

            if match:
                score = float(match.group(1))
            else:
                score = self.intact_interaction["Confidence value(s)"]

        pubmed = self.intact_interaction["Publication Identifier(s)"]

        sorted_participants = sorted([participant_a, participant_b])
        interaction_id = sorted_participants[0] + "__" + sorted_participants[1]
        interaction = Interaction(
            interaction_id,
            participants=[participant_a, participant_b],
            pubmed=pubmed,
            score=score
        )
        return interaction