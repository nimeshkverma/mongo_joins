from mongojoin import MongoJoin
from mongojoin import MongoCollection
from utils import pp, print_list

if __name__=="__main__":

	m_1 = MongoCollection("test","supplier",["supplier_id","name"],{"supplier_id":1001})
	m_2 = MongoCollection("test","order",["supplier_id","qty"],{"supplier_id":1001},host='localhost')

	c_1 = m_1.get_mongo_cursor()
	c_2 = m_2.get_mongo_cursor()

	print "************************ COLLECTION : SUPPLIER ************************"
	c = c_1.find({})
	print_list(c)

	print "************************ COLLECTION  : ORDER **************************"
	c = c_2.find({})
	print_list(c)

	aggregator = MongoJoin(m_1,m_2,["supplier_id"])

	print "\n************************ INNER JOIN **********************"
	pp(aggregator.inner())
	print "\n********************** LEFT OUTER JOIN **********************"
	pp(aggregator.left_outer())
	print "\n********************** RIGHT OUTER JOIN *********************"
	pp(aggregator.right_outer())
	print "\n********************** FULL OUTER JOIN *********************"
	pp(aggregator.full_outer())

