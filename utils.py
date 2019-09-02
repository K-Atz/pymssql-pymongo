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

def search_words(db, optype, words, connection):
    if db == MSSQL:
        return search_mssql(optype, words, connection)
    if db == MYSQL:
        return search_mysql(optype, words, connection)
    if db == MONGODB:
        return mongo_search(optype, words, connection)
    if db == ELASTIC:
        return elastic_search(optype, words, connection)
    if db == ELASTIC5:
        return elastic5_search(optype, words, connection)

#--------------------------------------------------------------------------------------------#

def search_mysql(optype, words, mysql_connection):
    mysql_cursor = mysql_connection.cursor()
    start = None
    end = None
    cnt = None
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
    mysql_cursor.execute("SELECT COUNT(*) as total_hits FROM %s WHERE MATCH(%s) AGAINST ('%s' IN BOOLEAN MODE)" % (TABLE, COLUMN, req))
    cnt = list(mysql_cursor)[0][0]
    end = datetime.datetime.now()
    return (cnt, end-start)

#--------------------------------------------------------------------------------------------#
'''
SELECT  *
FROM [Schema].[SomeFullTextIndexedView] t 
INNER JOIN CONTAINSTABLE([Schema].[SomeFullTextIndexedView], ColumnToSearch, '"*bla*" OR "*di*" OR "*bladi*"') c
    ON t.ID = c.[KEY]
ORDER BY [RANK] DESC

'''
def search_mssql(optype, words, mssql_conn):  
    mssql_cursor = mssql_conn.cursor()
    tempstr = ""
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
    cmd = "SELECT COUNT(*) FROM %s t INNER JOIN CONTAINSTABLE(%s, %s, '%s') c ON t._id = c.[KEY]" % (TABLE, TABLE, COLUMN, tempstr)
    start = datetime.datetime.now()
    mssql_cursor.execute(cmd)
    cnt = list(mssql_cursor)[0][0]
    end = datetime.datetime.now()
    return (cnt, end-start)

def search_mssql_old(optype, words, mssql_conn):  
    mssql_cursor = mssql_conn.cursor()
    cmd = 'SELECT COUNT(*) as total_hits FROM %s WHERE ' % TABLE
    if optype == SINGLE:
        cmd += "CONTAINS(%s,'%s')" % (COLUMN, words[0])
    elif optype == AND or optype == OR:
        op = None
        if optype == AND:
            op = 'AND'
        else:
            op = 'OR'
        tempstr = words[0]
        for i in range(1, len(words)):
            tempstr += " %s %s" % (op, words[i])
        cmd += "CONTAINS(%s,'%s')" % (COLUMN, tempstr)
    start = datetime.datetime.now()
    mssql_cursor.execute(cmd)
    cnt = list(mssql_cursor)[0][0]
    end = datetime.datetime.now()
    return (cnt, end-start)


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

def elastic_search(optype, words, es): #fieldname = 'abstract'
    body = {}
    if optype == SINGLE:
        body = {
            'size': MAX,
            "track_total_hits": True,
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
                'size': MAX,
                "track_total_hits": True,
                "query" : {
                    "bool" : {
                        "must" : search_words
                    }
                }
            }
        elif optype == OR:
            body = {
                'size': MAX,
                "track_total_hits": True,
                "query" : {
                    "bool" : {
                        "should" : search_words
                    }
                }
            }
    start = datetime.datetime.now()
    # result = es.search(index=DB, doc_type=TABLE, body=body)
    result = es.search(index=DB, body=body)
    # print('lennnnnnnnnnnnn', len(result['hits']['hits']))
    total_hits = result['hits']['total']['value']
    end = datetime.datetime.now()
    return (total_hits, end-start)

def mongo_search(optype, words, client):
    mydb = client[DB]
    mycol = mydb[TABLE]
    start = None
    end = None
    result = None
    total_hits = None
    if optype == SINGLE:
        start = datetime.datetime.now()
        result = mycol.count({"$text": {"$search": ("\"%s\"" % words[0])}})
        end = datetime.datetime.now()
    elif optype == AND:
        term = ""
        for i in range(0, len(words)-1):
            term += "\"%s\" " % words[i]
        term += "\"%s\"" % words[-1]
        start = datetime.datetime.now()
        result = mycol.count({"$text": {"$search": ("%s" % term)}})
        end = datetime.datetime.now()
    elif optype == OR:
        term = ""
        for i in range(0, len(words)-1):
            term += "%s " % words[i]
        term += "%s" % words[-1]
        start = datetime.datetime.now()
        result = mycol.count({"$text": {"$search": ("%s" % term)}})
        end = datetime.datetime.now()
    return (result, end-start)

def elastic5_search(optype, words, es): #fieldname = 'abstract'
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
    start = datetime.datetime.now()
    result = es.search(index=DB, doc_type=TABLE, body=body)
    end = datetime.datetime.now()
    total_hits = result['hits']['total']
    return (total_hits, end-start)