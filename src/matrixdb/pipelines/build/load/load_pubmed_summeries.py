import requests
import time
import json


def call_api_batch(pubmed_url, batch):
    pmids = ','.join(batch)
    response = requests.get(pubmed_url.replace('{pubmedid}', pmids))
    if response.status_code == 200:
        pubmed_summeries = json.loads(response.text)
        pubmeds_to_load = list()
        for pubmedid in pubmed_summeries['result'].keys():
            if pubmedid == 'uids':
                continue
            pub_data = pubmed_summeries['result'][pubmedid]
            title = '-'
            if 'title' in pub_data:
                title = pub_data['title']

            sorttitle = '-'
            if 'sorttitle' in pub_data:
                sorttitle = pub_data['sorttitle']
            authors = []
            if 'authors' in pub_data:
                authors = list(author['name'] for author in pub_data['authors'])
            date = '-'
            if 'pubdate' in pub_data:
                pubdate = pub_data['pubdate']

            if 'epubdate' in pub_data:
                epubdate = pub_data['epubdate']

            if 'source' in pub_data:
                source = pub_data['source']

            if 'fulljournalname' in pub_data:
                journal_name = pub_data['fulljournalname']

            if 'volume' in pub_data:
                volume = pub_data['volume']

            if 'issue' in pub_data:
                issue = pub_data['issue']

            if 'pages' in pub_data:
                pages = pub_data['pages']


            pubmeds_to_load.append({
                'id': pubmedid,
                'title': title,
                'sorttitle': sorttitle,
                'authors': authors,
                'source': source,
                'volume': volume,
                'issue': issue,
                'pages': pages,
                'journal': journal_name,
                'pubdate': pubdate,
                'epubdate': epubdate
            })
        print(len(pubmeds_to_load))
        return pubmeds_to_load
    else:
        print(f"API request failed for endpoint: {pubmed_url}")


def load_pmids(pmid_api_url, pmids, target_connection):
    batches = []
    for i in range(0, len(pmids), 20):
        batches.append(pmids[i:i + 20])

    for batch in batches:
        pubmeds_to_load = call_api_batch(pmid_api_url, batch)
        target_connection['publications'].insert_many(pubmeds_to_load)
        time.sleep(1)


def execute(config, database_manager):
    pmid_config = config["dependencies"]["pubmed"]
    api_url = pmid_config["api"]

    source_host = pmid_config["source"]["host"]
    source_port = pmid_config["source"]["port"]
    source_database = pmid_config["source"]["database"]

    target_host = pmid_config["target"]["host"]
    target_port = pmid_config["target"]["port"]
    target_database = pmid_config["target"]["database"]

    source_connection = database_manager.get_connection(
        database_name=source_database,
        host=source_host,
        port=source_port
    )

    target_connection = database_manager.get_connection(
        database_name=target_database,
        host=target_host,
        port=target_port
    )

    # Get all pubmedids
    pmids = list(source_connection["experiments"].distinct('pmid'))

    load_pmids(api_url, pmids, target_connection)