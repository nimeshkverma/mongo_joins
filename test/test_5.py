from mongojoin import MongoJoin
from mongojoin import MongoCollection
from utils import pp, print_list

if __name__ == "__main__":

    m_1 = MongoCollection("test", "supplier", [], {})

    c_1 = m_1.get_mongo_cursor()

    aggregator = MongoJoin(m_1, m_1, ["supplier_id"])

    print "\n************************ INNER JOIN **********************"
    pp(aggregator.inner())
