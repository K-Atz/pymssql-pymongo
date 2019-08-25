from datetime import datetime
from elasticsearch import Elasticsearch
import pymssql

SQL_EXPORT_DB = 'ricestexport'
RECORDS_COL = 'records'

es = Elasticsearch()

conn = pymssql.connect(server='172.16.8.10\RICESTSQLSERVER', user='sa', password='RICEST@SQLSERVER2008', database='NOSQL_db')  
cursor = conn.cursor()

cursor.execute('SELECT ID, DocID, Abstract FROM [NOSQL_db].[dbo].[Abstracts]')  
#cursor.execute('SELECT count(*) FROM [NOSQL_db].[dbo].[Abstracts]')  #4284322

sum = 0

for item in cursor:
    new_dict = {}
    new_dict['ID'] = item[0]
    new_dict['DocID'] = item[1]
    new_dict['Abstract'] = item[2]
    res = es.index(index=SQL_EXPORT_DB, doc_type=RECORDS_COL, body=new_dict)
    sum+=1
    print(sum, "items migrated to ES.")

es.indices.refresh(index=SQL_EXPORT_DB)


# doc = {
#     'author': 'kimchy',
#     'text': 'Elasticsearch: cool. bonsai cool.',
#     'timestamp': datetime.now(),
# }
# res = es.index(index="test-index", doc_type='tweet', id=1, body=doc)
# print(res['result'])

# doc2 = {
#     'author': 'kimchy2',
#     'text': 'Elasticsearch: cool. bonsai cool2.',
#     'timestamp': datetime.now(),
# }
# res = es.index(index="test-index", doc_type='tweet', id=2, body=doc2)
# print(res['result'])

# res = es.get(index="test-index", doc_type='tweet', id=1)
# print(res['_source'])

# es.indices.refresh(index="test-index")

# res = es.search(index="test-index", body={"query": {"match_all": {}}})
# print("Got Hits:" , res['hits']['total'])
# for hit in res['hits']['hits']:
#     print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])