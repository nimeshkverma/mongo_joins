import datetime
from pymongo import MongoClient, ReadPreference
from pymongo.errors import BulkWriteError
from bson.objectid import ObjectId
from bson.code import Code


class MongoConnections(object):

    "Class to connect to Mongo DB"

    def __init__(self):
    """
        Initializes Mongo Credentials to None
    """
        self.mongo_uri = None
        self.db_name = None
        self.collection = None

    def mongo_crendials(self, mongo_uri, db_name, collection_name):
    """
        Initializes Mongo Credentials given by user
        :param mongo_uri: Server and Port informations
        :type mongo_uri: string
        :param db_name: Name of the database
        :type db_name: string
        :param collection_name: Name of the collection
        :type collection_name: string
    """
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.collection = collection_name

    def get_mongo_cursor():
    """
        Returns Mongo cursor using the class variables
        :return: mongo collection for which cursor will be made
        :rtype: mongo colection object
    """
    try:
        client = MongoClient(self.mongo_uri)
        db = client[self.db_name]
        cursor = db[self.collection]
        return cursor
    except Exception as e:
        msg = "Mongo Connection could not be established for Mongo Uri: {mongo_uri}, Database: {db_name}, Collection {col}, Error: {error}".format(
            mongo_uri=self.mongo_uri, db_name=self.db_name, col=collection_name, error=str(e))
        raise Exception(msg)

    def get_bulk_cursor():
        """
            Returns the Bulk operation(unordered) cursor using the configuration stored in the config file
        """
        try:
            cursor = self.get_mongo_cursor()
            return cursor.initialize_unordered_bulk_op()
        except Exception as e:
            msg = "Mongo Bulk cursor could not be fetched, Error: {error}".format(
                error=str(e))
            raise Exception(msg)

    def bulk_cursor_execute(bulk_cursor):
        """
            Executes the bulk_cursor and handles exception

            :param bulk_cursor: The cursor to perform bulk operations
            :type bulk_cursor: pymongo bulk cursor object

            :returns: cursor to perform bulk operations, pymongo bulk cursor object
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
