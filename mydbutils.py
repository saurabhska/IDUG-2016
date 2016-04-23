from pymongo import MongoClient
import myconfig

# MongoClient defaults to the MongoDB instance that runs on the localhost interface on port 27017.
client = MongoClient()

#Initialize the database connection
db = client[myconfig.dbname]

def insert(collection,data):
	result = db[collection].insert(data)