from pymongo import MongoClient
from random import randint
from pymongo.errors import DuplicateKeyError


class Mongo:
    def __init__(self, db="domain", collection="listings"):
        client = MongoClient(host="127.0.0.1", port=27017)
        self.db = client[db]
        self.collection = self.db[collection]

    def insert_document(self, document):
        try:
            self.collection.insert_one(document)
        except DuplicateKeyError:
            return
