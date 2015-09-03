from utils import *
import pymongo

DB_NAME = "test"
COLL_NAME = "test"
SELECT_KEYS = "place"
WHERE_DICT = "{place:delhi}"

mc = MongoCollection(DB_NAME,COLL_NAME,SELECT_KEYS,WHERE_DICT)

k = mc.get_mongo_cursor()

print k