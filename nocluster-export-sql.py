import pymongo
import pymssql

SQL_EXPORT_DB = 'nosqlprj'
RECORDS_COL = 'records'

client = pymongo.MongoClient('mongodb://172.16.13.26:27023/')

mydb = client[SQL_EXPORT_DB]
mycol = mydb[RECORDS_COL]

conn = pymssql.connect(server='172.16.8.10\RICESTSQLSERVER', user='sa', password='RICEST@SQLSERVER2008', database='NOSQL_db')  
cursor = conn.cursor()
 
cursor.execute('SELECT ID, DocID, Abstract FROM [NOSQL_db].[dbo].[LangFilter]')

sum = 0

for item in cursor:
    new_dict = {}
    new_dict['_id'] = item[0]
    new_dict['DocID'] = item[1]
    new_dict['Abstract'] = item[2]
    mycol.insert_one(new_dict)
    sum+=1
    print(sum, "items migrated to Mongo.")