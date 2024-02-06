import logging
import time

from src.matrixdb.pipeline_manager.connection_provider import get_connection

'''
Pipeline for meta data loading
In case, pipeline-level configuration to be defined, ideally need to add a config for each pipeline and handle it loacally
'''


# psimi
def load_psimi(source, target):
    # Reading from source
    start = time.time()
    psimi_entries = list()
    try:
        psimi_entries = list({"id": psimi["id"], "name": psimi["name"]} for psimi in list(source["psimi"].find()))
        logging.info({
            "resource": "psimi",
            "source": "mongodb",
            "count": len(psimi_entries),
            "status": "READ_FROM_SOURCE_SUCCESS"
        })
        end = time.time() - start
    except Exception as e:
        end = time.time() - start
        logging.info({
            "resource": "psimi",
            "time": end,
            "status": "READ_FROM_SOURCE_FAILED"
        })

    # Loading to target
    start = time.time()
    try:
        target["psimi"].insert_many(psimi_entries)
        end = time.time() - start
        logging.info({
            "resource": "psimi",
            "count": len(psimi_entries),
            "time": end,
            "status": "LOAD_TO_TARGET_SUCCESS"
        })
    except Exception as e:
        logging.error({
            "error": str(e),
            "resource": "psimi",
            "time": end,
            "status": "LOAD_TO_TARGET_FAILED"
        })


# pubmed
def load_pubmed(source, target):
    publications = list()
    authors = set()
    start = time.time()
    try:
        for pubmed_entry in source["pubmed"].find():
            publication = pubmed_entry
            del publication["_id"]
            publications.append(publication)
        if len(publications) > 0:
            logging.info({
                "resource": "pubmed",
                "source": "mongodb",
                "count": len(publications),
                "status": "READ_FROM_SOURCE_SUCCESS"
            })
        else:
            logging.info({
                "resource": "pubmed",
                "source": "mongodb",
                "count": 0,
                "status": "READ_FROM_SOURCE_SUCCESS",
                "message": "No pubmeds read from the source"
            })
            return

    except Exception as e:
        end = time.time() - start
        logging.error({
            "error": str(e),
            "resource": "pubmed",
            "time": end,
            "status": "FAILED"
        })

    # Loading to the target
    try:
        target["pubmed"].insert_many(publications)
        logging.info({
            "resource": "pubmed",
            "source": "mongodb",
            "count": len(publications),
            "status": "READ_FROM_SOURCE"
        })
    except Exception as e:
        end = time.time() - start
        logging.error({
            "error": str(e),
            "resource": "pubmed",
            "time": end,
            "status": "FAILED"
        })


# GO
def load_go(source, target):
    start = time.time()
    go_entries = list()
    # Read from source
    try:
        for go_entry in source["go"].find():
            go = go_entry
            del go["_id"]
            go_entries.append(go)
        if len(go_entries) > 0:
            logging.info({
                "resource": "go",
                "source": "mongodb",
                "count": len(go_entries),
                "status": "READ_FROM_SOURCE_SUCCESS"
            })
        else:
            logging.info({
                "resource": "go",
                "source": "mongodb",
                "count": 0,
                "status": "READ_FROM_SOURCE_SUCCESS",
                "message": "No go entries read from source"
            })
            return

    except Exception as e:
        end = time.time() - start
        logging.error({
            "error": str(e),
            "resource": "go",
            "time": end,
            "status": "READ_FROM_SOURCE_FAILED"
        })

    # Load to target
    start = time.time()
    try:
        target["go"].insert_many(go_entries)
        end = time.time() - start
        logging.info({
            "resource": "go",
            "count": len(go_entries),
            "time": end,
            "status": "LOAD_TO_TARGET_SUCCESS"
        })
    except Exception as e:
        end = time.time() - start
        logging.error({
            "error": str(e),
            "resource": "go",
            "time": end,
            "status": "LOAD_TO_TARGET_FAILED"
        })


# uberon
def load_uberon(source, target):
    start = time.time()
    uberon_entries = list()
    # Reading from source
    try:
        for uberon_entry in source["uberon"].find():
            uberon = uberon_entry
            del uberon["_id"]
            uberon_entries.append(uberon)

        if len(uberon_entries) > 0:
            logging.info({
                "resource": "uberon",
                "source": "mongodb",
                "count": len(uberon_entries),
                "status": "READ_FROM_SOURCE_SUCCESS"
            })
        else:
            logging.info({
                "resource": "uberon",
                "source": "mongodb",
                "count": 0,
                "status": "READ_FROM_SOURCE_SUCCESS",
                "message": "No uberon entries read from source"
            })
            return
    except Exception as e:
        end = time.time() - start
        logging.error({
            "error": str(e),
            "resource": "uberon",
            "time": end,
            "status": "READ_FROM_SOURCE_FAILED"
        })

    # Load to target
    start = time.time()
    try:
        target["uberon"].insert_many(uberon_entries)
        end = time.time() - start
        logging.info({
            "resource": "uberon",
            "count": len(uberon_entries),
            "time": end,
            "status": "LOAD_TO_TARGET_SUCCESS"
        })
    except Exception as e:
        end = time.time() - start
        logging.info({
            "resource": "uberon",
            "count": len(uberon_entries),
            "time": end,
            "status": "LOAD_TO_TARGET_SUCCESS"
        })


# Brenda BTO
def load_bto(source, target):
    start = time.time()
    # Read from source
    brenda_entries = list()
    try:
        brenda_entries = list()
        for brenda_entry in source["bto"].find():
            brenda = brenda_entry
            brenda_entries.append(brenda)
        if len(brenda_entries) > 0:
            logging.info({
                "resource": "brenda",
                "source": "mongodb",
                "count": len(brenda_entries),
                "status": "READ_FROM_SOURCE_SUCCESS"
            })
        else:
            logging.info({
                "resource": "brenda",
                "source": "mongodb",
                "count": 0,
                "status": "READ_FROM_SOURCE_SUCCESS",
                "message": "No uberon entries read from source"
            })
            return
    except Exception as e:
        end = time.time() - start
        logging.error({
            "error": str(e),
            "resource": "brenda",
            "time": end,
            "status": "READ_FROM_SOURCE_FAILED"
        })

    # Load to target
    try:
        target["brenda"].insert_many(brenda_entries)
        end = time.time() - start
        logging.info({
            "resource": "brenda",
            "count": len(brenda_entries),
            "time": end,
            "status": "LOAD_TO_TARGET_SUCCESS"
        })
    except Exception as e:
        end = time.time() - start
        logging.error({
            "error": str(e),
            "resource": "brenda",
            "time": end,
            "status": "LOAD_TO_TARGET_FAILED"
        })


# Uniprot keywords
def load_uniprot_keywords(source, target):
    start = time.time()
    # Read from the source
    uniprot_keywords = list()
    try:
        for uniprot_keyword in source["uniprotKeywords"].find():
            uniprot_keywords.append({
                "id": uniprot_keyword["keyword"]["id"],
                "name": uniprot_keyword["keyword"]["name"],
                "definition": uniprot_keyword["definition"]
            })

        if len(uniprot_keywords) > 0:
            logging.info({
                "resource": "uniprot-keywords",
                "source": "mongodb",
                "count": len(uniprot_keywords),
                "status": "READ_FROM_SOURCE_SUCCESS"
            })
        else:
            logging.info({
                "resource": "uniprot-keywords",
                "source": "mongodb",
                "count": 0,
                "status": "READ_FROM_SOURCE_SUCCESS",
                "message": "No uberon entries read from source"
            })
            return
    except Exception as e:
        end = time.time() - start
        logging.error({
            "error": str(e),
            "resource": "uniprot-keywords",
            "time": end,
            "status": "READ_FROM_SOURCE_FAILED"
        })

    start = time.time()
    try:
        target["uniprotKeywords"].insert_many(uniprot_keywords)
        end = time.time() - start
        logging.info({
            "resource": "uniprot-keywords",
            "count": len(uniprot_keywords),
            "time": end,
            "status": "LOAD_TO_TARGET_SUCCESS"
        })
    except Exception as e:
        end = time.time() - start
        logging.error({
            "error": str(e),
            "resource": "uniprot-keywords",
            "time": end,
            "status": "LOAD_TO_TARGET_FAILED"
        })


def execute(config):
    source_connection = config["source"]["connection"]
    source_database = config["source"]["database"]
    source = get_connection(source_connection, source_database)

    target_connection = config["target"]["connection"]
    target_database = config["target"]["database"]
    target = get_connection(target_connection, target_database)

    load_psimi(source, target)
    load_go(source, target)
    load_uberon(source, target)
    load_pubmed(source, target)
    load_bto(source, target)
    load_uniprot_keywords(source, target)