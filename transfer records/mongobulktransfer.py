import pymongo
import pymssql

SQL_EXPORT_DB = 'nosqlprj'
RECORDS_COL = 'records'

client = pymongo.MongoClient('mongodb://172.16.13.26:27023/')

mydb = client[SQL_EXPORT_DB]
mycol = mydb[RECORDS_COL]

conn = pymssql.connect(server='172.16.8.10\RICESTSQLSERVER', user='sa', password='RICEST@SQLSERVER2008', database='NOSQL_db')  
cursor = conn.cursor()
 
cursor.execute('SELECT TOP(3000000) ID, DocID, Abstract FROM [NOSQL_db].[dbo].[LangFilterIEEE]')

sum = 0
all = []
bulks = 0
for item in cursor:
    new_dict = {}
    new_dict['_id'] = item[0]
    new_dict['DocID'] = item[1]
    new_dict['Abstract'] = item[2]
    all += [new_dict]
    sum+=1
    if sum == 50000:
        mycol.insert_many(all)
        sum = 0
        bulks += 1
        left = 60 - bulks
        print("\r%d bulks migrated to Mongo. %d left." % (bulks, left), end="")
        all = []