import datetime
from pymongo import MongoClient, ReadPreference
from pymongo.errors import BulkWriteError
from bson.objectid import ObjectId
from bson.code import Code


class MongoCollection(object):

    "Class to connect to Mongo DB"

    DEFAULT_MONGO_URI = 'mongodb://localhost:27017/'

    def __init__(self, db_name, collection_name, select_keys, where_dict={}, mongo_uri=DEFAULT_MONGO_URI):
        """
        Initializes Mongo Credentials given by user

        :param mongo_uri: Server and Port informations
        :type  mongo_uri: string

        :param db_name: Name of the database
        :type  db_name: string

        :param collection_name: Name of the collection
        :type  collection_name: string

        :param where_dict: 
        :type  where_dict: dictionary

        :param select_keys:
        :type  select_keys:

        """
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.collection = collection_name
        self.where_dict = where_dict
        self.select_keys = select_keys

    def get_mongo_cursor(self,bulk=False):
        """
            Returns Mongo cursor using the class variables
            
            :param bulk: bulk writer option
            :type bulk: boolean

            :return: mongo collection for which cursor will be made
            :rtype: mongo colection object
        """
        try:
            client = MongoClient(self.mongo_uri)
            db = client[self.db_name]
            cursor = db[self.collection]

            if bulk:
                try:
                    return cursor.initialize_unordered_bulk_op()
                except Exception as e:
                    msg = "Mongo Bulk cursor could not be fetched, Error: {error}".format(error=str(e))
                    raise Exception(msg)

            return cursor

        except Exception as e:
            msg = "Mongo Connection could not be established for Mongo Uri: {mongo_uri}, Database: {db_name}, Collection {col}, Error: {error}".format(
                mongo_uri=self.mongo_uri, db_name=self.db_name, col=collection_name, error=str(e))
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


class MongoAggregate(object):

    "Class to Aggregate on the collections"

    def __init__(self, collection1, collection2, group_by_keys, join_type="inner"):
        """
        Initializes Mongo Aggregate params to None

        :param join_type: Type of join operation to be performed
        :type  join_type: String

        """
        self.collection1 = collection1
        self.collection2 = collection2
        self.group_by_keys = group_by_keys
        self.join_type = join_type

    
    def build_mongo_doc(self, key_list):
        """
            :param key_list
            :type  key_list: list
        """
        mongo_doc = {}
        if isinstance(key_list,list) and key_list:
            for key in key_list:
                mongo_doc[key] = "$" + str(key)

        return mongo_doc

    
    def build_pipeline(self, collection):
        """
        """
        pipeline = []

        if isinstance(collection.where_dict,dict) and collection.where_dict:
            match_dict = {
                "$match": collection.where_dict
            }
            pipeline.append(match_dict)

        group_keys_dict = self.build_mongo_doc(self.group_by_keys)
        push_dict = self.build_mongo_doc(collection.select_keys)

        group_by_dict = {
            "_id": group_by_keys,
            "docs": {
                "$push": push_dict
            }
        }
        pipeline.append(group_by_dict)

        return pipeline

    
    def fetch_and_process_data(self, collection, pipeline):
        """
            Fetches and Processes data from the input collection by aggregating using the pipeline

            :param collection_name: The collection name for which mongo connection has to be build
            :type collection_name: string
            :param pipeline: The pipeline using which aggregation will be performed
            :type collection_name: list of dicts

            :returns: dict of property_id,metric_count
        """
        grouped_docs = list(collection.get_mongo_cursor.aggregate(pipeline))
        grouped_docs_dict = {}

        while grouped_docs:
            doc = grouped_docs.pop()
            keys_list = []
            for group_by_key in self.group_by_keys:
                keys_list.append(doc["_id"].get(group_by_key, None))
                
        grouped_docs_dict[set(keys_list)] = grouped_docs["docs"]

        return grouped_docs_dict

    
    def fetch_and_merge(self):
        docs_dicts = []

        for collection in [self.collection1, self.collection2]:
            docs_dicts.append(self.build_pipeline(collection))

        for key in docs_dicts[0]:
            if docs_dicts[1].get(key):
                docs_dicts[0][key] + docs_dicts[1][key]
                del docs_dicts[1][key]

        docs_dicts[0].update(docs_dicts[1])
        del docs_dicts[1]

        return docs_dicts[0]

    
    def join_results(self):
        return self.fetch_and_merge()
