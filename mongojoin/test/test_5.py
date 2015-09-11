import os
import sys

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path = [os.path.join(SCRIPT_DIR + '/../')] + sys.path

from mongojoin import MongoJoin
from mongojoin import MongoCollection
from utils import print_dict, print_list

if __name__ == "__main__":

    m_1 = MongoCollection("test", "supplier", [], {})

    c_1 = m_1.get_mongo_cursor()

    aggregator = MongoJoin(m_1, m_1, ["supplier_id"])

    print "\n************************ INNER JOIN **********************"
    print_dict(aggregator.inner())
