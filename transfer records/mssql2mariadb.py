import pymongo
import pymssql
import mysql.connector

conn = pymssql.connect(server='172.16.8.10\RICESTSQLSERVER', user='sa', password='RICEST@SQLSERVER2008', database='NOSQL_db')  
cursor = conn.cursor()

mysql_connection = mysql.connector.connect(host='localhost', user="root", passwd="password", db='nosqlprj')
cursor2 = mysql_connection.cursor()

 
cursor.execute('SELECT ID, DocID, Abstract FROM [NOSQL_db].[dbo].[LangFilter]') #2 Mil Records

sum = 0
bulk = 0
partition = 0
for item in cursor:
    cursor2.execute("INSERT INTO records (_id, DocID, Abstract) VALUES (%s, %s, %s)" , (str(item[0]), str(item[1]), item[2]))
    sum+=1
    if sum == 1000:
        sum = 0
        bulk+=1
        mysql_connection.commit()
        print("Bulk Count: %d" % bulk)

