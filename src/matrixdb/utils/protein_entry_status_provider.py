class ProteinStatusProvider:

    def __init__(self, source_connection):
        self.source_connection = source_connection

    def get_protein_entry_status(self, accessions):
        accession_status = {a:
            {
                'accession': a,
                'obsolete': True
            } for a in accessions}

        # Check in uniprot
        uniprots_found = list(self.source_connection['uniprotEntries'].find({
            "accession.text": {
                '$in': accessions
            }
        }))

        if len(uniprots_found) > 0:

            for accession in accession_status:
                for uniprot_found in uniprots_found:
                    if type(uniprot_found['accession']) is list:
                        uniprot_accession = uniprot_found['accession'][0]['text']

                        if accession == uniprot_accession:
                            accession_status[accession] = {
                                'accession': uniprot_accession,
                                'primary': True,
                                'entry': uniprot_found,
                                'uniprot': True
                            }
                        elif accession in list(u['text'] for u in uniprot_found['accession']):
                            accession_status[accession] = {
                                'accession': uniprot_accession,
                                'obsolete': True,
                                'primaryAccession': uniprot_found['accession'][0]['text'],
                                'entry': uniprot_found,
                                'uniprot': True
                            }
                    else:
                        uniprot_accession = uniprot_found['accession']
                        if accession == uniprot_accession:
                            accession_status[accession] = {
                                'accession': accession,
                                'primary': True,
                                'entry': uniprot_found,
                                'uniprot': True
                            }


        # Check in trembl
        trembls_found = self.source_connection["tremblEntries"].find({
            "primaryAccession": {
                '$in': accessions
            }
        })

        for accession in accession_status:
            for trembl_found in trembls_found:
                if trembl_found == accession:
                    accession_status[accession] = {
                        'accession': accession,
                        'primary': True,
                        'entry': trembl_found,
                        'trembl': True
                    }

        return accession_status