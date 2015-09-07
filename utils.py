import datetime
from pymongo import MongoClient, ReadPreference
from pymongo.errors import BulkWriteError
from bson.objectid import ObjectId
from bson.code import Code
from collections import Counter


class MongoCollection(object):

    "Class to connect to Mongo DB"

    DEFAULT_MONGO_URI = 'mongodb://localhost:27017/'
    DEFAULT_PORT = 27017

    def __init__(self, db_name, collection_name, select_keys=[], where_dict={}, host=None, port=None, mongo_uri=DEFAULT_MONGO_URI):
        """
        Initializes Mongo Credentials given by user

        :param mongo_uri: Server and Port informations
        :type  mongo_uri: string

        :param db_name: Name of the database
        :type  db_name: string

        :param collection_name: Name of the collection
        :type  collection_name: string

        :param where_dict: Filters  (Date range etc.)
        :type  where_dict: dictionary

        :param select_keys: Keys to be fetched after merging
        :type  select_keys: list (priority)

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

            :return: mongo collection for which cursor will be made
            :rtype: mongo colection object
        """
        try:
            if self.host:
                if self.port:
                    client = MongoClient(self.host,self.port)
                else:
                    client = MongoClient(self.host,MongoCollection.DEFAULT_PORT)
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


class CollectionsProcessedData(object):

    "Class to Fetch and Process data from the collections"

    def __init__(self, left_collection, right_collection, group_by_keys=[]):
        """
        Initializes Mongo Aggregate params to None

        :param join_type: Type of join operation to be performed
        :type  join_type: String

        :param group_by_keys: Attributes on which the join is to be performed
        :type  group_by_keys: list

        """
        self.left_collection = left_collection
        self.right_collection = right_collection
        self.group_by_keys = group_by_keys
        self.collections_data = {}

    def build_mongo_doc(self, key_list):
        """
            :param key_list
            :type  key_list: list

            :returns mongo_doc: dict
        """
        mongo_doc = {}

        if isinstance(key_list, list) and key_list:

            for key in key_list:
                mongo_doc[key] = "$" + str(key)
        return mongo_doc

    def build_pipeline(self, collection):
        """
            :param collection:  
            :type  collection: MongoCollection

            :return pipeline: list of dicts
        """
        pipeline = []

        if isinstance(collection.where_dict, dict) and collection.where_dict:
            match_dict = {
                "$match": collection.where_dict
            }
            pipeline.append(match_dict)

        group_keys_dict = self.build_mongo_doc(self.group_by_keys)
        push_dict = self.build_mongo_doc(collection.select_keys)

        group_by_dict = {
            "$group":
                {
                    "_id": group_keys_dict,
                    "docs": {
                        "$push": push_dict
                    }
                }
        }

        pipeline.append(group_by_dict)

        return pipeline

    def fetch_and_process_data(self, collection, pipeline):
        """
            Fetches and Processes data from the input collection by aggregating using the pipeline

            :param collection: The collection name for which mongo connection has to be build
            :type  collection: MongoCollection

            :param pipeline: The pipeline using which aggregation will be performed
            :type  pipeline: list of dicts

            :returns: dict of property_id,metric_count
        """
        collection_cursor = collection.get_mongo_cursor()
        grouped_docs = list(collection_cursor.aggregate(pipeline))
        grouped_docs_dict = {}

        while grouped_docs:
            doc = grouped_docs.pop()
            keys_list = []

            for group_by_key in self.group_by_keys:
                keys_list.append(doc["_id"].get(group_by_key, None))
            grouped_docs_dict[tuple(keys_list)] = doc['docs']

        return grouped_docs_dict

    def get_collections_data(self):
        collections = {
            'left': self.left_collection,
            'right': self.right_collection
        }
        for collection_type, collection in collections.iteritems():
            pipeline = self.build_pipeline(collection)
            self.collections_data[collection_type] = self.fetch_and_process_data(collection, pipeline)


class MongoJoins(CollectionsProcessedData):

    "Class to perform Inner Join on collections"

    def inner(self):
        self.get_collections_data()
        for key in self.collections_data['left'].keys():
            if self.collections_data['right'].get(key):
                self.collections_data['left'][key] += \
                    self.collections_data['right'].pop(key, [])
            else:
                del self.collections_data['left'][key]
        return self.collections_data['left']

    def left_outer(self):
        self.get_collections_data()

        for key in self.collections_data['left']:
            self.collections_data['left'][key] += \
                self.collections_data['right'].pop(key, [])

        return self.collections_data['left']

    def right_outer(self):
        self.get_collections_data()

        for key in self.collections_data['right']:
            self.collections_data['right'][key] += \
                self.collections_data['left'].pop(key, [])

        return self.collections_data['right']

    def full_outer(self):
        self.get_collections_data()

        for key in self.collections_data['left']:
            self.collections_data['left'][key] += \
                self.collections_data['right'].pop(key, [])

        for key in self.collections_data['right']:
            self.collections_data['left'][
                key] = self.collections_data['right'][key]

        return self.collections_data['left']
