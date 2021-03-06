import os
from arango import ArangoClient


class Arango:
    """
    Client to work with ArangoDB
    """

    def __init__(self, database="domain", collection="listings"):
        """
        Class constructor
        @param database: arango database name
        @param collection: arango collection name
        """
        self.__client = ArangoClient(hosts=os.getenv("ARANGO_HOST"))
        sys_db = self.__client.db("_system", username="", password="")
        if "domain" not in sys_db.databases():
            sys_db.create_database(database)
            print("Creating database")
        self.db = self.__client.db("domain", username="", password="")
        if "listings" not in [c["name"] for c in self.db.collections()]:
            self.db.create_collection(collection)
            print("Creating listings collection")
        self.collection = self.db.collection(collection)

    def __del__(self):
        """
        Class destructor
        @return: None
        """
        self.__client.close()
        print("Closing DB Collection")
