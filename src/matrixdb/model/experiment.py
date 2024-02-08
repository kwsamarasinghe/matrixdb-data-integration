class Experiment:

    def __init__(self, experiment_id, participants,
                 source, pmid, interaction_detection_method, interaction_type,
                 host_organisms, xrefs, annotations, parameters, expansion_method,
                 creation_date, update_date):
        self.experiment_id = experiment_id
        self.participants = participants
        self.source = source
        self.pmid = pmid
        self.interaction_detection_method = interaction_detection_method
        self.interaction_type = interaction_type
        self.host_organisms = host_organisms
        self.xrefs = xrefs
        self.annotations = annotations
        self.parameters = parameters
        self.expansion_method = expansion_method
        self.creation_date = creation_date
        self.update_date = update_date

        self.associations = {
            "direct": {
                "binary": list(),
                "spoke_exapneded_to": list()
            },
            "inferred": list()
        }

    def add_supporting_interaction(self, interaction_id):
        self.directly_supports.append(interaction_id)

    def add_spoke_expanded_into_interaction(self, interaction_id):
        self.spoke_expanded_into.append(interaction_id)

    def add_xref(self, xref_key, xref_value):
        self.xrefs[xref_key] = xref_value

    def get_id(self):
        return self.experiment_id

    def get_expansion_method(self):
        return self.expansion_method

    def to_json(self):
        return {
            "id": self.experiment_id,
            "participants": self.participants,
            "source": self.source,
            "pmid": self.pmid,
            "interaction_detection_method": self.interaction_detection_method,
            "interaction_type": self.interaction_type,
            "host_organisms": self.host_organisms,
            "xrefs": self.xrefs,
            "annotations": self.annotations,
            "parameters": self.parameters,
            "expansion_method": self.expansion_method,
            "creation_date": self.creation_date,
            "update_date": self.update_date
        }


