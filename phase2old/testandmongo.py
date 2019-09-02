import pymssql, pymongo, random, copy, time, mysql.connector, datetime
from nltk.corpus import stopwords 
from nltk.tokenize import word_tokenize 
from elasticsearch import Elasticsearch
from elasticsearch5 import Elasticsearch as Elasticsearch5

HOSTIP = '172.16.13.26'
DB = 'nosqlprj'
TABLE = 'records'
COLUMN = 'Abstract'
MAX = 1
TIMEOUT = 100000
AND = 'and'
OR = 'or'
SINGLE = 'single'

SOURCE_TABLE = "[NOSQL_db].[dbo].[LangFilter]"
NUMBER_OF_RECORDS = 2000000
STOPWORDS = set(stopwords.words('english'))

MSSQL = 'sql server'
MYSQL = 'mysql'
MONGODB = 'mongodb'
ELASTIC = 'elastic'
ELASTIC5 = 'elastic5'



def mongo_search(optype, words, client):
    mydb = client[DB]
    mycol = mydb[TABLE]
    start = None
    end = None
    result = None
    total_hits = None
    if optype == SINGLE:
        start = datetime.datetime.now()
        result = mycol.find({"$text": {"$search": ("\"%s\"" % words[0])}})
        end = datetime.datetime.now()
    elif optype == AND:
        term = ""
        for i in range(0, len(words)-1):
            term += "\"%s\" " % words[i]
        term += "\"%s\"" % words[-1]
        start = datetime.datetime.now()
        result = mycol.find({"$text": {"$search": ("%s" % term)}})
        end = datetime.datetime.now()
    elif optype == OR:
        term = ""
        for i in range(0, len(words)-1):
            term += "%s " % words[i]
        term += "%s" % words[-1]
        start = datetime.datetime.now()
        result = mycol.find({"$text": {"$search": ("%s" % term)}})
        end = datetime.datetime.now()
    return (result, end-start)

words = ['iran', 'dose', 'equal']
client = pymongo.MongoClient('mongodb://%s:27023/' % HOSTIP)
result = mongo_search(AND, words, client)[0]

sum0 = 0
sum1 = 0
for item in result:
    sum0 += 1
    l = item['Abstract'].lower()
    if (words[0] in l) and (words[1] in l) and (words[2] in l):
        sum1 += 1
        # print(item['Abstract'])
        # print("-----------------------------------------")

if sum0 == sum1:
    print("equal!")
