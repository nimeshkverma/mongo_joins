# mongojoin
**Joins on Mongo**

This is an attempt to provide people using MongoDB - and fetching related data from different collections rather frequently - with a library to combine data from more than one collection (*Read 'joins'*)

MongoDB is a  de-normalized database and does not offer methods to perform "join" action on collections.

Feel free to raise issues and contribute !

Preliminary documentation can be found in **DOCUMENTATION.md**



## Installation
***
To install the package, type the following -

	pip install mongojoin


## Sample data - Populating MongoDb with sample data
***
Navigate to `test/sample_data` in the `mongojoin` directory and type the following command -

	> mongoimport --dbname test --collection order --file order.json
	> mongoimport --dbname test --collection supplier --file supplier.json

This will create and populate the required collections with sample data for running the tests.


The two collections 'supplier' and 'order' will be used to demonstrate how to use mongojoin.
To check the contents of the collection, the following command can be used on the MongoDB shell :

	> use test
	> db.supplier.find({})
	> db.order.find({})

## Using `mongojoin` to perform joins on two MongoDB collections
***
Type the following in Python shell to import the necessary modules - 

	>>> from mongojoin.mongojoin import MongoJoin
	>>> from mongojoin.mongojoin import MongoCollection

To create `MongoCollection` objects for the two collections to be joined, type the following -

	>>> collection_1 = MongoCollection("test", "supplier", ["supplier_id", "name"], {"supplier_id": 1001})
	>>> collection_2 = MongoCollection("test", "order", ["supplier_id", "qty"], {"supplier_id": 1001}, host='localhost')

A `MongoJoin` object needs to be created to perform a join on the two `MongoCollection` objects.
To create the `MongoJoin` object, type the following -

	>>> aggregator = MongoJoin(collection_1, collection_2, ["supplier_id"])


The join commands return a `DefaultDict` object which can be used accordingly.
To perform inner join, 
	
	>>> aggregator.inner()

To perform left-outer join,
	
	>>> aggregator.left_outer()

To perform right-outer join,
	
	>>> aggregator.right_outer()

To perform full-outer join,
	
	>>> aggregator.full_outer()

The join commands return a `DefaultDict` object which can be used accordingly.

This is equivalent to the following query in SQL:

    SELECT supplier.supplier_id, supplier.name, order.supplier_id, order.qty FROM supplier JOIN order ON supplier.supplier_id = order.supplier_id WHERE supplier.supplier_id = 1001 AND order.supplier_id = 1001


More contents here - https://pypi.python.org/pypi/mongojoin/1.0.0