from pymongo import MongoClient


class DatabaseManager:

    def __init__(self):
        self.connection_clients = dict()

    def get_connection(self, database_name, host, port):
        if f'{host}_{port}' in self.connection_clients:
            return self.connection_clients[f'{host}_{port}'][database_name]

        # Create a new database connection
        client = MongoClient(
            host=host,
            port=int(port),
            maxPoolSize=10
        )
        self.connection_clients[f'{host}_{port}'] = client
        return client[database_name]

