import pymongo
import pymssql

conn = pymssql.connect(server='172.16.8.10\RICESTSQLSERVER', user='sa', password='RICEST@SQLSERVER2008', database='NOSQL_db')  
cursor = conn.cursor()

conn2 = pymssql.connect(server='172.16.13.26', user='sa', password='MSSql-pwd', database='ricestexport')  
cursor2 = conn2.cursor()
 
cursor.execute('SELECT ID, DocID, Abstract FROM [NOSQL_db].[dbo].[Abstracts]')  
#cursor.execute('SELECT count(*) FROM [NOSQL_db].[dbo].[Abstracts]')  #4284322

sum = 0

for item in cursor:
    cursor2.execute("INSERT INTO [ricestexport].[dbo].[records] VALUES (%d, %d, %s)", (item[0], item[1], item[2]))
    sum+=1
    print(sum, "items migrated.")

conn2.commit()