import datetime
import copy
from pymongo import MongoClient, ReadPreference
from pymongo.errors import BulkWriteError
from bson.objectid import ObjectId
from bson.code import Code
from collections import defaultdict


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

<<<<<<< HEAD
    def __init__(self, collection1, collection2, group_by_keys =[]):
=======
    def __init__(self, left_collection, right_collection, group_by_keys=[]):
>>>>>>> origin/master
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
<<<<<<< HEAD
=======
        self.collections_data = {}
>>>>>>> origin/master

    def build_mongo_doc(self, key_list):
        """
            :param key_list
            :type  key_list: list

            :returns mongo_doc: dict
        """
        mongo_doc = {}
<<<<<<< HEAD
        #print key_list
        #print isinstance(key_list,list)

        if isinstance(key_list,list) and key_list:
=======

        if isinstance(key_list, list) and key_list:
>>>>>>> origin/master

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
<<<<<<< HEAD
        push_dict       = self.build_mongo_doc(collection.select_keys)

        group_by_dict = { 
=======
        push_dict = self.build_mongo_doc(collection.select_keys)

        group_by_dict = {
>>>>>>> origin/master
            "$group":
                {
                    "_id": group_keys_dict,
                    "docs": {
<<<<<<< HEAD
                    "$push": push_dict
                    }
                }
            }
=======
                        "$push": push_dict
                    }
                }
        }
>>>>>>> origin/master

        pipeline.append(group_by_dict)

        return pipeline

    def fetch_and_process_data(self, collection, pipeline):
        """
            Fetches and Processes data from the input collection by aggregating using the pipeline

            :param collection: The collection name for which mongo connection has to be build
            :type  collection: MongoCollection

            :param pipeline: The pipeline using which aggregation will be performed
            :type  pipeline: list of dicts

            :return grouped_docs_dict: dict of property_id,metric_count
        """
        collection_cursor = collection.get_mongo_cursor()
<<<<<<< HEAD
        #print collection_cursor
=======
>>>>>>> origin/master
        grouped_docs = list(collection_cursor.aggregate(pipeline))
        grouped_docs_dict = {}
        #print grouped_docs
        while grouped_docs:
            doc = grouped_docs.pop()
            keys_list = []

            for group_by_key in self.group_by_keys:
                #print group_by_key,1000
                keys_list.append(doc["_id"].get(group_by_key, None))
<<<<<<< HEAD
                #print keys_list
=======
>>>>>>> origin/master
            grouped_docs_dict[tuple(keys_list)] = doc['docs']

        return grouped_docs_dict

    def get_collections_data(self):

        collections = {
            'left': self.left_collection,
            'right': self.right_collection
        }
        for collection_type, collection in collections.iteritems():
            pipeline = self.build_pipeline(collection)
            self.collections_data[collection_type] = self.fetch_and_process_data(
                collection, pipeline)


class MongoJoins(CollectionsProcessedData):

<<<<<<< HEAD
        for collection in [self.collection1, self.collection2]:
            #print collection
            pipeline = self.build_pipeline(collection)
            
            #print pipeline,20
            docs_dicts.append(self.fetch_and_process_data(collection,pipeline))
        return docs_dicts
=======
    "Class to perform Inner Join on collections"

    def change_dict_keys(self, data_dict, prefix):
        """
            :param data_dict: dictionary which is to be altered
            :type  data_dict: dict

            :param prefix: prefix to be attached before every key
            :type  prefix: string

            :return dict_: dict
        """
        keys = data_dict.keys()
        dummy_dict = copy.deepcopy(data_dict)
        changed_dict = {}
        for key in keys:
            changed_dict[prefix + str(key)] = dummy_dict.pop(key)
        return changed_dict

    def generate_join_docs_list(self, left_collection_list, right_collection_list):
        """
            :param left_collection_list: Left Collection to be joined
            :type  left_collection_list: MongoCollection

            :param right_collection_list: Right Collection to be joined
            :type  right_collection_list: MongoCollection

            :return joined_docs: List of docs post join
        """
        joined_docs = []
        if (len(left_collection_list) != 0) and (len(right_collection_list) != 0):
            for left_doc in left_collection_list:
                for right_doc in right_collection_list:
                    l_dict = self.change_dict_keys(left_doc, 'L_')
                    r_dict = self.change_dict_keys(right_doc, 'R_')
                    joined_docs.append(dict(l_dict, **r_dict))
        elif left_collection_list:
            for left_doc in left_collection_list:
                joined_docs.append(self.change_dict_keys(left_doc, 'L_'))
        else:
            for right_doc in right_collection_list:
                joined_docs.append(self.change_dict_keys(right_doc, 'R_'))

        return joined_docs

    def merge_join_docs(self, keys):
        """
            :param left_collection_list: 
            :type  left_collection_list: MongoCollection

            :return join: dict
        """
        join = defaultdict(list)

        for key in keys:
            join[key] = self.generate_join_docs_list(
                self.collections_data['left'].get(key, []), self.collections_data['right'].get(key, []))
        return join

    def inner(self):
        """
            Function to perform Inner Join
            :return inner_join: dict
        """
        self.get_collections_data()
        inner_join = self.merge_join_docs(set(self.collections_data['left'].keys()) & set(
            self.collections_data['right'].keys()))
        return inner_join

    def left_outer(self):
        """
            Function to perform Left Outer Join
            :return left_outer: dict
        """
        self.get_collections_data()
        left_outer_join = self.merge_join_docs(
            set(self.collections_data['left'].keys()))
        return left_outer_join

    def right_outer(self):
        """
            Function to perform Right Outer Join
            :return right_outer: dict
        """
        self.get_collections_data()
        right_outer_join = self.merge_join_docs(
            set(self.collections_data['right'].keys()))
        return right_outer_join

    def full_outer(self):
        """
            Function to perform Full Outer Join
            :return full_outer: dict
        """
        self.get_collections_data()
        full_outer_join = self.merge_join_docs(
            set(self.collections_data['left'].keys()) | set(self.collections_data['right'].keys()))
        return full_outer_join
<<<<<<< HEAD



>>>>>>> origin/master
=======
>>>>>>> master
