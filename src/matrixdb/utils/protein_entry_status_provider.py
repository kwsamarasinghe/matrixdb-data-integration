import logging


class ProteinStatusProvider:

    def __init__(self, source_connection):
        self.source_connection = source_connection

    def get_protein_entry_status(self, accessions):
        logging.info({
            'message': f'Checking status for {len(accessions)}'
        })
        accessions = sorted(accessions)
        accession_status = {a: {} for a in accessions}

        # Check in uniprot
        uniprots_found = list(self.source_connection['uniprotEntries'].find({
            "accession.text": {
                '$in': accessions
            }
        }).sort("accession.text"))

        logging.info({
            'message': f'Uniprots found {len(uniprots_found)} / {len(accessions)}'
        })

        if len(uniprots_found) > 0:
            for uniprot_found in uniprots_found:
                if type(uniprot_found['accession']) is list:
                    uniprot_accession = uniprot_found['accession'][0]['text']
                    if uniprot_accession in accession_status:
                        accession_status[uniprot_accession] = {
                            'accession': uniprot_accession,
                            'primary': True,
                            'entry': uniprot_found,
                            'uniprot': True
                        }
                        print('status found')
                    else:
                        for accession in list(u['text'] for u in uniprot_found['accession']):
                            if accession in accession_status:
                                accession_status[accession] = {
                                    'accession': accession,
                                    'obsolete': True,
                                    'primaryAccession': uniprot_found['accession'][0]['text'],
                                    'entry': uniprot_found,
                                    'uniprot': True
                                }
                                print('status found')
                else:
                    uniprot_accession = uniprot_found['accession']['text']
                    if uniprot_accession in accession_status:
                        accession_status[uniprot_accession] = {
                            'accession': uniprot_accession,
                            'primary': True,
                            'entry': uniprot_found,
                            'uniprot': True
                        }
                        print('status found')

        logging.info({
            'message': f'Uniprots processed'
        })

        # Check in trembl
        unmatched_accessions = [accession for accession in accession_status if accession_status[accession] == {}]
        trembls_found = list(self.source_connection["tremblEntries"].find({
            "primaryAccession": {
                '$in': unmatched_accessions
            }
        }).sort("primaryAccession"))

        logging.info({
            'message': f'Trembls found {len(trembls_found)} / {len(unmatched_accessions)}'
        })

        for trembl_found in trembls_found:
            if trembl_found['primaryAccession'] in accession_status:
                accession_status[trembl_found['primaryAccession']] = {
                    'accession': trembl_found['primaryAccession'],
                    'primary': True,
                    'entry': trembl_found,
                    'trembl': True
                }
                print('status found')

        for accession in accession_status:
            if accession_status[accession] == {}:
                accession_status[accession] = {
                    'accession': accession,
                    'obsolete': True
                }

        logging.info({
            'message': f'Trembls processed'
        })
        return list(accession_status.values())