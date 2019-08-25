import pymongo
import pymssql


# username = 'nosql-admin'
# password = 'nosql-pwd'
SQL_EXPORT_DB = 'ricestexport'
RECORDS_COL = 'records'

# client = pymongo.MongoClient('mongodb://%s:%s@172.16.13.26:27021/admin' % (username, password))
client = pymongo.MongoClient('mongodb://172.16.13.26:27023/')

mydb = client[SQL_EXPORT_DB]
mycol = mydb[RECORDS_COL]

conn = pymssql.connect(server='172.16.8.10\RICESTSQLSERVER', user='sa', password='RICEST@SQLSERVER2008', database='NOSQL_db')  
cursor = conn.cursor()
 
cursor.execute('SELECT ID, DocID, Abstract FROM [NOSQL_db].[dbo].[Abstracts]')  
#cursor.execute('SELECT count(*) FROM [NOSQL_db].[dbo].[Abstracts]')  #4284322

sum = 0

for item in cursor:
    new_dict = {}
    new_dict['ID'] = item[0]
    new_dict['DocID'] = item[1]
    new_dict['Abstract'] = item[2]
    mycol.insert_one(new_dict)
    sum+=1
    print(sum, "items migrated to Mongo.")