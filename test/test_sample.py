import os
import sys

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path = [os.path.join(SCRIPT_DIR + '/../')] + sys.path
from utils import *


def test_mongo_connections():
    post_obj = MongoCollection(
        'tumblelog', 'post', ['email', "Roll no"], {"email": "h@example.com"}, 'mongodb://10.1.6.211:27017/')
    user_obj = MongoCollection('tumblelog', 'user', [
                               'email', "first_name", "last_name"], {"email": "h@example.com"}, 'mongodb://10.1.6.211:27017/')
    return post_obj, user_obj

print test_mongo_connections()


def test_collections_processed_data():
    post_obj, user_obj = test_mongo_connections()
    collections_processed_data_obj = CollectionsProcessedData(
        post_obj, user_obj, ['email'])
    collections_processed_data_obj.get_collections_data()
    print collections_processed_data_obj.group_by_keys
    for ctype in collections_processed_data_obj.collections_data:
        print ctype
        for doc, value in collections_processed_data_obj.collections_data[ctype].iteritems():
            print doc, value
        print "*******************"


test_collections_processed_data()


def test_MongoJoins():
    post_obj, user_obj = test_mongo_connections()
    mongo_join_obj = MongoJoins(
        post_obj, user_obj, ['email'])
    print 'inner'
    for x, v in mongo_join_obj.inner().iteritems():
        print x, v
    print 'left'
    for x, v in mongo_join_obj.left_outer().iteritems():
        print x, v
    print 'right'
    for x, v in mongo_join_obj.right_outer().iteritems():
        print x, v
    print 'full'
    for x, v in mongo_join_obj.full_outer().iteritems():
        print x, v

test_MongoJoins()
