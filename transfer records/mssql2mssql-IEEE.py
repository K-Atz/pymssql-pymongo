import pymongo
import pymssql


conn = pymssql.connect(server='172.16.14.74', user='sa', password='SQLSERVER@74', database='DigitalLibrary')  
cursor = conn.cursor()

conn2 = pymssql.connect(server='172.16.8.10\RICESTSQLSERVER', user='sa', password='RICEST@SQLSERVER2008', database='NOSQL_db')  
cursor2 = conn2.cursor()


 
cursor.execute('SELECT Abstract FROM [DigitalLibrary].[dbo].[RIS_IEEE] WHERE Abstract is not null') #2 Mil Records

sum = 0
_id = 2684029

for item in cursor:
    cursor2.execute("INSERT INTO [NOSQL_db].[dbo].[LangFilterIEEE] VALUES (%d, %d, %s)", (_id, _id, item[0]))
    sum+=1
    _id+=1
    print(sum, "items migrated.")

conn2.commit()