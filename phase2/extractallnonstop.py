import pymssql, pymongo, random
from nltk.corpus import stopwords 
from nltk.tokenize import word_tokenize 
from elasticsearch import Elasticsearch
import copy

HOSTIP = '172.16.13.26'
DB = 'nosqlprj'
TABLE = 'records'
COLUMN = 'Abstract'
MAX = 1
TIMEOUT = 100000
AND = 1
OR = 2
SINGLE = 0

SOURCE_TABLE = "[NOSQL_db].[dbo].[LangFilter]"
NUMBER_OF_RECORDS = 2000000
STOPWORDS = set(stopwords.words('english'))

conn = pymssql.connect(server='172.16.8.10\RICESTSQLSERVER', user='sa', password='RICEST@SQLSERVER2008', database='NOSQL_db')  
cursor = conn.cursor()
es = Elasticsearch('172.16.13.26')

client = pymongo.MongoClient('mongodb://172.16.13.26:27023/')
mydb = client[DB]
mycol = mydb[TABLE]

word1 = 'data'
word2 = 'computer'

def containnumber(word):
    for i in range(0, 10):
        if str(i) in word:
            return True
    return False

def randomword(n):
    cmd = "SELECT Abstract FROM (SELECT ROW_NUMBER() OVER (ORDER BY ID ASC) AS rownumber, Abstract FROM %s) AS foo WHERE rownumber = %d"  % (SOURCE_TABLE, random.randint(1, NUMBER_OF_RECORDS)) 
    cursor.execute(cmd)
    for item in cursor:
        word_tokens = word_tokenize(item[0])
    w = word_tokens[random.randint(0, len(word_tokens)-1)]
    if (len(w) >= n and (w.lower() not in STOPWORDS) and not containnumber(w)):
        return w.lower()
    return randomword(n)

def elastic_search(optype, words, fieldname): #fieldname = 'abstract'
    body = {}
    if optype == SINGLE:
        body = {
            # "timeout": TIMEOUT,
            "size": MAX,
            "query" : {
                "match" : {
                    fieldname : words[0]
                }
            }
        }
    else:
        search_words = []
        for w in words:
            search_words += [
                {
                    "match" : {
                        fieldname : w
                    }
                }
            ]
        if optype == AND:
            body = {
                # "timeout": TIMEOUT,
                "size": MAX,
                "query" : {
                    "bool" : {
                        "must" : search_words
                    }
                }
            }
        elif optype == OR:
            body = {
                # "timeout": TIMEOUT,
                "size": MAX,
                "query" : {
                    "bool" : {
                        "should" : search_words
                    }
                }
            }
    return es.search(index=DB, doc_type=TABLE, body=body)

# word1 = randomword(3)
# print(word1)
# word2 = randomword(3)
# word3 = randomword(3)
# print(word1, word2, word3)
# r = elastic_search(AND, [word1, word2, word3], COLUMN)
# r = elastic_search(SINGLE, [word1], COLUMN)
# print("total hits: ",r['hits']['total'])
# print("total fetched: ",len(r['hits']['hits']))
# for hit in r['hits']['hits']:
#     print("\nscore: %f\ntext: %s" % (hit['_score'],hit['_source']['Abstract'].lower()))
#     print("\n")

def mongo_search(optype, words):
    result = None
    if optype == SINGLE:
        result = mycol.find({"$text": {"$search": ("\"%s\"" % words[0])}})
    elif optype == AND:
        term = ""
        for i in range(0, len(words)-1):
            term += "\"%s\" " % words[i]
        term += "\"%s\"" % words[-1]
        result = mycol.find({"$text": {"$search": ("%s" % term)}})
    elif optype == OR:
        term = ""
        for i in range(0, len(words)-1):
            term += "%s " % words[i]
        term += "%s" % words[-1]
        result = mycol.find({"$text": {"$search": ("%s" % term)}})
    return result

# r = mongo_search(AND, [word1, word2])
r = mongo_search(SINGLE, [word1])
rr = copy.copy(r)
print("total mongo hits: ", len(list(r)))
for item in rr:
    print(item)
    break

# r = elastic_search(AND, [word1, word2], COLUMN)
r = elastic_search(SINGLE, [word1], COLUMN)
print("total es hits: ",r['hits']['total'])