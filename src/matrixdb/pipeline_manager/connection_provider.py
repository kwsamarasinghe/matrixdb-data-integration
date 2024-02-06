from pymongo import MongoClient


def get_connection(connection_string, database):
    connection_string = connection_string
    return MongoClient(connection_string)[database]
