# import pandas as pd
# import json as js
# import elasticsearch as es
import pymssql
import pymongo

conn = pymssql.connect(server='172.16.8.10\RICESTSQLSERVER', user='sa', password='RICEST@SQLSERVER2008', database='NOSQL_db')  
cursor = conn.cursor()  

col_names = []
cursor.execute("select Column_name from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='Abstracts'")
# cursor.execute("SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('ricestdb.dbo.Abstracts')")
for item in cursor:
    col_names += [str(item[0])]

# cursor.execute('SELECT count(*) FROM [ricestdb].[dbo].[Abstracts] where [Language_ID]=2 and Abstract is not null and DATALENGTH(Abstract)>5')  

cursor.execute('SELECT TOP 20 ID, DocID, Abstract FROM [NOSQL_db].[dbo].[Abstracts]')  
#cursor.execute('SELECT count(*) FROM [NOSQL_db].[dbo].[Abstracts]')  #4284322

print(col_names)
for item in cursor:
    print(item)
    print("\n")