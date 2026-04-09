
class BiomoleculeCacheProvider:

    def __init__(self, db_connection):
        self.ecm_biomolecule_cache = dict()
        ecm_count = 0
        for biomolecule in db_connection["biomolecules"].find():

            if biomolecule["type"] == 'protein':
                if "ecm" in biomolecule and biomolecule["ecm"] is True:
                    #print(biomolecule)
                    self.ecm_biomolecule_cache[biomolecule["id"]] = biomolecule["id"]
                    ecm_count += 1
                    continue

                if "ecmness" in biomolecule:
                    self.ecm_biomolecule_cache[biomolecule["id"]] = biomolecule["id"]
                    ecm_count += 1
                    continue

            if biomolecule["type"] == 'gag' or biomolecule["type"] == 'smallmol' or biomolecule["type"] == "multimer"\
                    or biomolecule["type"] == 'spep' or biomolecule["type"] == 'pfrag' or biomolecule["type"] == 'cat' or biomolecule["type"] == 'lipid':
                ecm_count += 1
                self.ecm_biomolecule_cache[biomolecule["id"]] = biomolecule["id"]
                if biomolecule["type"] == 'spep':
                    if "xrefs" in biomolecule and "ebi" in biomolecule["xrefs"]:
                        self.ecm_biomolecule_cache[biomolecule["xrefs"]["ebi"]] = biomolecule["id"]
                    if "xrefs" in biomolecule and "chebi" in biomolecule["xrefs"]:
                        self.ecm_biomolecule_cache[biomolecule["xrefs"]["chebi"]] = biomolecule["id"]
                if "xrefs" in biomolecule:
                    if "chebi" in biomolecule["xrefs"]:
                        self.ecm_biomolecule_cache[biomolecule["xrefs"]["chebi"]] = biomolecule["id"]
                    if "complex_portal" in biomolecule["xrefs"]:
                        self.ecm_biomolecule_cache[biomolecule["xrefs"]["complex_portal"]] = biomolecule["id"]
                    if "uniprot" in biomolecule["xrefs"]:
                        self.ecm_biomolecule_cache[biomolecule["xrefs"]["uniprot"]] = biomolecule["id"]

    def is_ecm_biomolecule(self, biomolecule_id):
        return biomolecule_id in self.ecm_biomolecule_cache