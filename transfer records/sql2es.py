from datetime import datetime
from elasticsearch import Elasticsearch
import pymssql

SQL_EXPORT_DB = 'nosqlprj'
RECORDS_COL = 'records'

es = Elasticsearch('172.16.13.26')

conn = pymssql.connect(server='172.16.8.10\RICESTSQLSERVER', user='sa', password='RICEST@SQLSERVER2008', database='NOSQL_db')  
cursor = conn.cursor()

cursor.execute('SELECT TOP(10) ID, DocID, Abstract FROM [NOSQL_db].[dbo].[LangFilter]')

sum = 0

for item in cursor:
    new_dict = {}
    new_dict['DocID'] = item[1]
    new_dict['Abstract'] = item[2]
    res = es.index(index=SQL_EXPORT_DB, doc_type=RECORDS_COL, id=item[0], body=new_dict)
    sum+=1
    print(sum, "items migrated to ES.")

es.indices.refresh(index=SQL_EXPORT_DB)