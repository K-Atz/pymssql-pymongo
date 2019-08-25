import pymongo
import pymssql


username = 'nosql-admin'
password = 'nosql-pwd'
SQL_EXPORT_DB = 'sql-export'
RECORDS_COL = 'records'

# SQL_EXPORT_DB = 'sql-export-sample'
# SQL_EXPORT_DB = 'sql-export-sample-nosharding'
# RECORDS_COL = 'sample'

client = pymongo.MongoClient('mongodb://%s:%s@172.16.13.26:27017/admin' % (username, password))
# print(client.list_database_names())

mydb = client[SQL_EXPORT_DB]
mycol = mydb[RECORDS_COL]

conn = pymssql.connect(server='172.16.8.10\RICESTSQLSERVER', user='sa', password='RICEST@SQLSERVER2008', database='NOSQL_db')  
cursor = conn.cursor()
 
cursor.execute('SELECT TOP 100000 ID, DocID, Abstract FROM [NOSQL_db].[dbo].[Abstracts]')  
#cursor.execute('SELECT count(*) FROM [NOSQL_db].[dbo].[Abstracts]')  #4284322

sum = 0

for item in cursor:
    new_dict = {}
    new_dict['_id'] = item[0]
    new_dict['doc_id'] = item[1]
    new_dict['abstract'] = item[2]
    mycol.insert_one(new_dict)
    sum+=1

print("Total items migrated: %d" % sum)