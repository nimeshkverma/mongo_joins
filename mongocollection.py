import datetime
from pymongo import MongoClient, ReadPreference
from pymongo.errors import BulkWriteError
from bson.objectid import ObjectId
from bson.code import Code


class MongoCollection(object):

    "Connects to Mongo DB"

    DEFAULT_MONGO_URI = 'mongodb://localhost:27017/'
    DEFAULT_PORT = 27017

    def __init__(self, db_name, collection_name, select_keys=[], where_dict={}, host=None, port=None, mongo_uri=DEFAULT_MONGO_URI):
        """
        Initializes Mongo Credentials given by user

        :param mongo_uri: Mongo Server and Port information
        :type  mongo_uri: string

        :param db_name: Name of the database
        :type  db_name: string

        :param collection_name: Name of the collection
        :type  collection_name: string

        :param where_dict: Filters  (Date range etc.)
        :type  where_dict: dictionary

        :param select_keys: Key, Value pairs to be fetched after join
        :type  select_keys: list

        """
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.collection = collection_name
        self.where_dict = where_dict
        self.select_keys = select_keys
        self.host = host
        self.port = port

    def get_mongo_cursor(self, bulk=False):
        """
            Returns Mongo cursor using the class variables

            :param bulk: bulk writer option
            :type bulk: boolean

            :return: mongo collection for which cursor will be created
            :rtype: mongo colection object
        """
        try:
            if self.host:
                if self.port:
                    client = MongoClient(self.host, self.port)
                else:
                    client = MongoClient(
                        self.host, MongoCollection.DEFAULT_PORT)
            else:

                client = MongoClient(self.mongo_uri)

            db = client[self.db_name]
            cursor = db[self.collection]

            if bulk:
                try:
                    return cursor.initialize_unordered_bulk_op()
                except Exception as e:
                    msg = "Mongo Bulk cursor could not be fetched, Error: {error}".format(
                        error=str(e))
                    raise Exception(msg)

            return cursor

        except Exception as e:
            msg = "Mongo Connection could not be established for Mongo Uri: {mongo_uri}, Database: {db_name}, Collection {col}, Error: {error}".format(
                mongo_uri=self.mongo_uri, db_name=self.db_name, col=self.collection, error=str(e))
            raise Exception(msg)

    def bulk_cursor_execute(self, bulk_cursor):
        """
            Executes the bulk_cursor

            :param bulk_cursor: Cursor to perform bulk operations
            :type bulk_cursor: pymongo bulk cursor object

            :returns: pymongo bulk cursor object (for bulk operations)
        """
        try:
            result = bulk_cursor.execute()
        except BulkWriteError as bwe:
            msg = "bulk_cursor_execute: Exception in executing Bulk cursor to mongo with {error}".format(
                error=str(bwe))
            raise Exception(msg)
        except Exception as e:
            msg = "Mongo Bulk cursor could not be fetched, Error: {error}".format(
                error=str(e))
            raise Exception(msg)
