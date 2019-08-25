import pymssql

conn = pymssql.connect(server='172.16.8.10\RICESTSQLSERVER', user='sa', password='RICEST@SQLSERVER2008', database='NOSQL_db')  
cursor = conn.cursor()
 
#cursor.execute('SELECT TOP 100000 ID, DocID, Abstract FROM [NOSQL_db].[dbo].[Abstracts]')  
#cursor.execute('SELECT count(*) FROM [NOSQL_db].[dbo].[Abstracts]')  #4284322

cursor.execute("SELECT DATA_TYPE, Column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'LangFilter'")

for item in cursor:
    print(item[0], item[1])