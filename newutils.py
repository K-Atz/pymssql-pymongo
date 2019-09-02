import pymssql, pymongo, random, copy, time, mysql.connector, datetime
from nltk.corpus import stopwords 
from nltk.tokenize import word_tokenize 
from elasticsearch import Elasticsearch
from elasticsearch5 import Elasticsearch as Elasticsearch5

#--------------------------------------------------------------------------------------------#

HOSTIP = '172.16.13.26'
DB = 'nosqlprj'
TABLE = 'records'
COLUMN = 'Abstract'
# TIMEOUT = 100000
AND = 'and'
OR = 'or'
SINGLE = 'single'
MAX = 100

SOURCE_TABLE = "[NOSQL_db].[dbo].[LangFilter]"
NUMBER_OF_RECORDS = 2000000
STOPWORDS = set(stopwords.words('english'))

MSSQL = 'sql server'
MYSQL = 'mysql'
MONGODB = 'mongodb'
ELASTIC = 'elastic'
ELASTIC5 = 'elastic5'

#--------------------------------------------------------------------------------------------#

def containnumber(word):
    for i in range(0, 10):
        if str(i) in word:
            return True
    return False

conn = pymssql.connect(server='172.16.8.10\RICESTSQLSERVER', user='sa', password='RICEST@SQLSERVER2008', database='NOSQL_db')  
cursor = conn.cursor()
def randomword(n,m):
    cmd = "SELECT Abstract FROM (SELECT ROW_NUMBER() OVER (ORDER BY ID ASC) AS rownumber, Abstract FROM %s) AS foo WHERE rownumber = %d"  % (SOURCE_TABLE, random.randint(1, NUMBER_OF_RECORDS)) 
    cursor.execute(cmd)
    for item in cursor:
        word_tokens = word_tokenize(item[0])
    w = word_tokens[random.randint(0, len(word_tokens)-1)]
    if (w.isalpha() and len(w) <= m and len(w) >= n and (w.lower() not in STOPWORDS)):
        return w.lower()
    return randomword(n,m)

#--------------------------------------------------------------------------------------------#

def mysql_search(optype, words, mysql_connection):
    mysql_cursor = mysql_connection.cursor()
    start = None
    end = None
    elapsed_time = None
    req = ""
    if optype == SINGLE:
        req += '+' + words[0]
    elif optype == AND:
        for i in range(0, len(words)-1):
            req += '+%s ' % words[i]
        req += '+%s' % words[-1]
    elif optype == OR:
        for i in range(0, len(words)-1):
            req += '%s ' % words[i]
        req += '%s' % words[-1]
    start = datetime.datetime.now()
    tempstr0 = ""
    if optype == SINGLE:
        tempstr0 = "%s" % words[0]
    else:
        tempstr0 = "%s" % words[0]
        for i in range(1, len(words)):
            tempstr0 +=",%s" % words[i]
    tempstr1 = "MATCH(%s) AGAINST ('%s' IN NATURAL LANGUAGE MODE)" % (COLUMN, tempstr0)
    tempstr2 = "MATCH(%s) AGAINST ('%s' IN BOOLEAN MODE)" % (COLUMN, req)
    mysql_cursor.execute("SELECT %s as score, _id FROM %s WHERE %s ORDER BY score DESC" % (tempstr1, TABLE, tempstr2))
    end = datetime.datetime.now()
    elapsed_time = end - start
    f=open("mysqlresults.txt", "a+")
    start = datetime.datetime.now()
    temp_str = ""
    for w in words:
        temp_str += w + " "
    print("---------------\nWORDS: " + temp_str + "\n\n%s RESULT ITEMS:" % optype, file= f)

    sum = 0
    for item in mysql_cursor:
        sum += 1
        print("SCORE: %f | ITEM NUMBER %d | ITEM ID: %s" % (item[0], sum, item[1]), file= f)
    print("---------------", file= f)
    end = datetime.datetime.now()
    elapsed_time += (end - start)
    td = elapsed_time.total_seconds()
    return (sum, td)

#--------------------------------------------------------------------------------------------#

def mssql_search(optype, words, mssql_conn):  
    mssql_cursor = mssql_conn.cursor()
    tempstr = ""
    elapsed_time = None
    if optype == SINGLE:
        tempstr += words[0]
    elif optype == AND or optype == OR:
        op = None
        if optype == AND:
            op = 'AND'
        else:
            op = 'OR'
        tempstr = words[0]
        for i in range(1, len(words)):
            tempstr += " %s %s" % (op, words[i])
    cmd = "SELECT RANK, t._id FROM %s t INNER JOIN CONTAINSTABLE(%s, %s, '%s') c ON t._id = c.[KEY] ORDER BY [RANK] DESC" % (TABLE, TABLE, COLUMN, tempstr)
    start = datetime.datetime.now()
    mssql_cursor.execute(cmd)
    end = datetime.datetime.now()
    elapsed_time = end - start
    f=open("mssqlresults.txt", "a+")
    start = datetime.datetime.now()
    temp_str = ""
    for w in words:
        temp_str += w + " "
    print("---------------\nWORDS: " + temp_str + "\n\n%s RESULT ITEMS:" % optype, file = f)

    sum = 0
    for item in mssql_cursor:
        sum += 1
        print("SCORE: %f | ITEM NUMBER %d | ITEM ID: %s" % (item[0], sum, item[1]), file = f)
    print("---------------", file = f)
    end = datetime.datetime.now()
    elapsed_time += (end - start)
    td = elapsed_time.total_seconds()
    return (sum, td)

#--------------------------------------------------------------------------------------------#

def mongo_search(optype, words, client):
    mydb = client[DB]
    mycol = mydb[TABLE]
    start = None
    end = None
    result = None
    total_hits = None
    elapsed_time = None
    if optype == SINGLE:
        start = datetime.datetime.now()
        result = mycol.find({"$text": {"$search": ("\"%s\"" % words[0])}},{"score": { "$meta": "textScore" }}).sort([('score', {'$meta': 'textScore'})])
        end = datetime.datetime.now()
    elif optype == AND:
        term = ""
        for i in range(0, len(words)-1):
            term += "\"%s\" " % words[i]
        term += "\"%s\"" % words[-1]
        start = datetime.datetime.now()
        result = mycol.find({"$text": {"$search": ("%s" % term)}},{"score": { "$meta": "textScore" }}).sort([('score', {'$meta': 'textScore'})])
        end = datetime.datetime.now()
    elif optype == OR:
        term = ""
        for i in range(0, len(words)-1):
            term += "%s " % words[i]
        term += "%s" % words[-1]
        start = datetime.datetime.now()
        result = mycol.find({"$text": {"$search": ("%s" % term)}},{"score": { "$meta": "textScore" }}).sort([('score', {'$meta': 'textScore'})])
        end = datetime.datetime.now()

    elapsed_time = end - start
    f=open("mongoresults.txt", "a+")
    start = datetime.datetime.now()
    temp_str = ""
    for w in words:
        temp_str += w + " "
    print("---------------\nWORDS: " + temp_str + "\n\n%s RESULT ITEMS:" % optype, file=f)

    sum = 0
    for item in result:
        sum += 1
        print("SCORE: %f | ITEM NUMBER %d | ITEM ID: %s" % (item['score'], sum, item['_id']), file=f)
    print("---------------", file=f)
    end = datetime.datetime.now()
    elapsed_time += (end - start)
    td = elapsed_time.total_seconds()
    return (sum, td)

def elastic5_search(optype, words, es):
    body = {}
    if optype == SINGLE:
        body = {
            "size": MAX,
            "query" : {
                "match" : {
                    COLUMN : words[0]
                }
            }
        }
    else:
        search_words = []
        for w in words:
            search_words += [
                {
                    "match" : {
                        COLUMN : w
                    }
                }
            ]
        if optype == AND:
            body = {
                "size": MAX,
                "query" : {
                    "bool" : {
                        "must" : search_words
                    }
                }
            }
        elif optype == OR:
            body = {
                "size": MAX,
                "query" : {
                    "bool" : {
                        "should" : search_words
                    }
                }
            }

    f=open("es5results.txt", "a+")
    start = datetime.datetime.now()

    page = es.search(index = DB,doc_type = TABLE,scroll = '2m',body=body)
    sid = page['_scroll_id']
    hits_count = page['hits']['total']

    temp_str = ""
    for w in words:
        temp_str += w + " "
    print("---------------\nWORDS: " + temp_str + "\n\n%s RESULT ITEMS:" % optype, file=f)

    sum = 0
    while True:
        all_hits = page['hits']['hits']
        for item in all_hits:
            sum += 1
            print("SCORE: %f | ITEM NUMBER %d | ITEM ID: %s" % (item['_score'], sum, item['_id']), file=f)
        if sum == hits_count:
            break
        page = es.scroll(scroll_id = sid, scroll = '2m')
        sid = page['_scroll_id']

    print("---------------", file=f)
    end = datetime.datetime.now()
    td = (end-start).total_seconds()
    return (hits_count, td)

def elastic7_search(optype, words, es):
    body = {}
    if optype == SINGLE:
        body = {
            "size": MAX,
            "query" : {
                "match" : {
                    COLUMN : words[0]
                }
            }
        }
    else:
        search_words = []
        for w in words:
            search_words += [
                {
                    "match" : {
                        COLUMN : w
                    }
                }
            ]
        if optype == AND:
            body = {
                "size": MAX,
                "query" : {
                    "bool" : {
                        "must" : search_words
                    }
                }
            }
        elif optype == OR:
            body = {
                "size": MAX,
                "query" : {
                    "bool" : {
                        "should" : search_words
                    }
                }
            }

    f=open("es7results.txt", "a+")
    start = datetime.datetime.now()

    page = es.search(index = DB,scroll = '2m',body=body)
    sid = page['_scroll_id']
    hits_count = page['hits']['total']['value']

    temp_str = ""
    for w in words:
        temp_str += w + " "
    print("---------------\nWORDS: " + temp_str + "\n\n%s RESULT ITEMS:" % optype, file=f)

    sum = 0
    while True:
        all_hits = page['hits']['hits']
        for item in all_hits:
            sum += 1
            print("SCORE: %f | ITEM NUMBER %d | ITEM ID: %s" % (item['_score'], sum, item['_id']), file=f)
        if sum == hits_count:
            break
        page = es.scroll(scroll_id = sid, scroll = '2m')
        sid = page['_scroll_id']

    print("---------------", file=f)
    end = datetime.datetime.now()
    td = (end-start).total_seconds()
    return (hits_count, td)