class CollectionsProcessedData(object):

    "Fetches and Processes data from the collections"

    def __init__(self, left_collection, right_collection, join_keys=[]):
        """
            Initializes collection params

            :param left_collection: Left collection object on which join is to be performed
            :type  left_collection: MongoCollection object

            :param right_collection: Right collection object on which join is to be performed
            :type  right_collection: MongoCollection object

            :param join_keys: Attributes on which the join is to be performed
            :type  join_keys: list

        """
        self.left_collection = left_collection
        self.right_collection = right_collection
        self.join_keys = join_keys
        self.collections_data = {}

    def build_mongo_doc(self, key_list):
        """
            Creates the components of aggregation pipeline
            :param key_list: list of key which will be used to create the components of aggregation pipeline
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
            Creates aggregation pipeline for aggregation
            :param collection: Mongo collection for aggregation
            :type  collection: MongoCollection

            :return pipeline: list of dicts
        """
        pipeline = []

        if isinstance(collection.where_dict, dict) and collection.where_dict:
            match_dict = {
                "$match": collection.where_dict
            }
            pipeline.append(match_dict)

        group_keys_dict = self.build_mongo_doc(self.join_keys)
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
            Fetches and Processess data from the input collection by aggregating using the pipeline

            :param collection: The collection object for which mongo connection has to be made
            :type  collection: MongoCollection

            :param pipeline: The pipeline using which aggregation will be performed
            :type  pipeline: list of dicts

            :return grouped_docs_dict: dict of property_id,metric_count
        """
        collection_cursor = collection.get_mongo_cursor()
        grouped_docs = list(collection_cursor.aggregate(pipeline))
        grouped_docs_dict = {}

        while grouped_docs:
            doc = grouped_docs.pop()
            keys_list = []

            for group_by_key in self.join_keys:
                keys_list.append(doc["_id"].get(group_by_key, None))
            grouped_docs_dict[tuple(keys_list)] = doc['docs']

        return grouped_docs_dict

    def get_collections_data(self):
        """
            Driver function to fetch the data from the two collections
        """

        collections = {
            'left': self.left_collection,
            'right': self.right_collection
        }
        for collection_type, collection in collections.iteritems():
            pipeline = self.build_pipeline(collection)
            self.collections_data[collection_type] = self.fetch_and_process_data(
                collection, pipeline)
