import pymssql

conn = pymssql.connect(server='172.16.8.10\RICESTSQLSERVER', user='sa', password='RICEST@SQLSERVER2008', database='NOSQL_db')  
cursor = conn.cursor()
cursor.execute('SELECT TOP (50) ID, DocID, Abstract FROM [NOSQL_db].[dbo].[LangFilter]') #2 Mil Records

sum=0
with open("allabstracts.txt", "a") as myfile:
    for item in cursor:
        myfile.write("\n%s" % item[2])
        sum+=1
        print("Total: %d" % sum)