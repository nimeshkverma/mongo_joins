import datetime
import copy
from collections import defaultdict
from processdata import CollectionsProcessedData
from mongocollection import MongoCollection

class MongoJoin(CollectionsProcessedData):

    "Perform all Joins on collections"

    def change_dict_keys(self, data_dict, prefix):
        """
            Prefixes 'L_'/'R_' to the collection keys
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
            Helper function for merge_join_docs
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
            Merges the final list of docs
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
            Performs Inner Join
            :return inner_join: dict
        """
        self.get_collections_data()
        inner_join = self.merge_join_docs(set(self.collections_data['left'].keys()) & set(
            self.collections_data['right'].keys()))
        return inner_join

    def left_outer(self):
        """
            Performs Left Outer Join
            :return left_outer: dict
        """
        self.get_collections_data()
        left_outer_join = self.merge_join_docs(
            set(self.collections_data['left'].keys()))
        return left_outer_join

    def right_outer(self):
        """
            Performs Right Outer Join
            :return right_outer: dict
        """
        self.get_collections_data()
        right_outer_join = self.merge_join_docs(
            set(self.collections_data['right'].keys()))
        return right_outer_join

    def full_outer(self):
        """
            Performs Full Outer Join
            :return full_outer: dict
        """
        self.get_collections_data()
        full_outer_join = self.merge_join_docs(
            set(self.collections_data['left'].keys()) | set(self.collections_data['right'].keys()))
        return full_outer_join
