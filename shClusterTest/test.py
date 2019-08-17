import pymongo

username = 'nosql-admin'
password = 'nosql-pwd'
client = pymongo.MongoClient('mongodb://%s:%s@172.16.13.26:27017/admin' % (username, password))
print(client.list_database_names())
print('test')