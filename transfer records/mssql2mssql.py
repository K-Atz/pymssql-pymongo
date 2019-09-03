import pymongo
import pymssql

conn = pymssql.connect(server='172.16.8.10\RICESTSQLSERVER', user='sa', password='RICEST@SQLSERVER2008', database='NOSQL_db')  
cursor = conn.cursor()

conn2 = pymssql.connect(server='172.16.13.26', user='sa', password='MSSql-pwd', database='nosqlprj')  
cursor2 = conn2.cursor()
 
cursor.execute('SELECT TOP(3000000) ID, DocID, Abstract FROM [NOSQL_db].[dbo].[LangFilterIEEE]') #2 Mil Records

sum = 0
bulk = 0
partition = 0
for item in cursor:
    cursor2.execute("INSERT INTO [nosqlprj].[dbo].[records] VALUES (%d, %d, %s)", (item[0], item[1], item[2]))
    sum+=1
    if sum == 1000:
        sum = 0
        bulk+=1
        conn2.commit()
        print("\rMSSQL Bulk Count: %d" % bulk, end="")