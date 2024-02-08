class ProteinStatusProvider:

    def __init__(self, source_connection):
        self.source_connection = source_connection

    def get_protein_entry_status(self, accession):
        # Check in uniprot
        uniprots_found = list(self.source_connection['uniprotEntries'].find({
            "accession.text": accession
        }))

        if len(uniprots_found) > 0:

            for uniprot_found in uniprots_found:
                if type(uniprot_found['accession']) is list:
                    uniprot_accession = uniprot_found['accession'][0]['text']

                    if accession == uniprot_accession:
                        return {
                            'accession': accession,
                            'primary': True,
                            'entry': uniprot_found,
                            'uniprot': True
                        }
                    elif accession in list(u['text'] for u in uniprot_found['accession']):
                        return {
                            'accession': accession,
                            'obsolete': True,
                            'primaryAccession': uniprot_found['accession'][0]['text'],
                            'entry': uniprot_found,
                            'uniprot': True
                        }
                else:
                    uniprot_accession = uniprot_found['accession']
                    if accession == uniprot_accession:
                        return {
                            'accession': accession,
                            'primary': True,
                            'entry': uniprot_found,
                            'uniprot': True
                        }


        # Check in trembl
        trembl_found = self.source_connection["tremblEntries"].find_one({
            "primaryAccession": accession
        })

        if trembl_found is not None:
            return {
                'accession': accession,
                'primary': True,
                'entry': trembl_found,
                'trembl': True
            }
        else:
            return {
                'accession': accession,
                'obsolete': True
            }