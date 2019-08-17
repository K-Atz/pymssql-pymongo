import pymongo
import pymssql


username = 'nosql-admin'
password = 'nosql-pwd'
SQL_EXPORT_DB = 'sql-export'
RECORDS_COL = 'records'

client = pymongo.MongoClient('mongodb://%s:%s@172.16.13.26:27021/admin' % (username, password))

mydb = client[SQL_EXPORT_DB]
mycol = mydb[RECORDS_COL]

conn = pymssql.connect(server='172.16.13.26', user='sa', password='MSSql-pwd', database='sqlexport')  
cursor = conn.cursor()
 
cursor.execute('SELECT TOP 100 ID, DocID, Abstract FROM [sqlexport].[dbo].[records]')  
#cursor.execute('SELECT count(*) FROM [NOSQL_db].[dbo].[Abstracts]')  #4284322

sum = 0

for item in cursor:
    print(item)
    sum+=1

print("Total items migrated: %d" % sum)