from mongojoin import MongoJoin
from mongojoin import MongoCollection
from utils import print_dict, print_list

if __name__ == "__main__":

    m_1 = MongoCollection("test", "supplier", ["supplier_id", "name"], {})

    c_1 = m_1.get_mongo_cursor()

    print "************************ COLLECTION : SUPPLIER ************************"
    c = c_1.find({})
    print_list(c)

    aggregator = MongoJoin(m_1, m_1, ["supplier_id"])

    print "\n************************ INNER JOIN **********************"
    print_dict(aggregator.inner())
    print "\n********************** LEFT OUTER JOIN **********************"
    print_dict(aggregator.left_outer())
    print "\n********************** RIGHT OUTER JOIN *********************"
    print_dict(aggregator.right_outer())
    print "\n********************** FULL OUTER JOIN *********************"
    print_dict(aggregator.full_outer())
