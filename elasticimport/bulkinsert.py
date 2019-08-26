from datetime import datetime
import pymssql
from elasticsearch import Elasticsearch
from elasticsearch import helpers

es = Elasticsearch('172.16.13.26')

conn = pymssql.connect(server='172.16.8.10\RICESTSQLSERVER', user='sa', password='RICEST@SQLSERVER2008', database='NOSQL_db')  
cursor = conn.cursor()

cursor.execute('SELECT ID, DocID, Abstract FROM [NOSQL_db].[dbo].[LangFilter]') #2 Mil Records
sum = 0
actions = []
for item in cursor:
    actions += [
        {
            "_index": "nosqlprj",
            "_type": "records",
            "_id": item[0],
            "_source": {
                "DocID": item[1],
                "Abstract": item[2]
            }
        }
    ]
    sum+=1
    print("%d items gathered." % sum)

print("Bulk insert . . .")
helpers.bulk(es, actions)
