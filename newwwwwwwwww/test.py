import pymongo
import pymssql

SQL_EXPORT_DB = 'nosqlprj'
RECORDS_COL = 'records'

# client = pymongo.MongoClient('mongodb://172.16.13.26:27023/')
# mydb = client[SQL_EXPORT_DB]
# mycol = mydb[RECORDS_COL]

client_3sh = pymongo.MongoClient('mongodb://172.16.13.26:27030/', retryWrites=False)
mydb_3sh = client_3sh[SQL_EXPORT_DB]
mycol_3sh = mydb_3sh[RECORDS_COL]

mycol_3sh.insert({'text': 'hooyyy'})

for item in mycol_3sh.find():
    print (item['text'])