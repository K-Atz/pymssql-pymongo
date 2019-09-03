from newutils import *

es5_client = Elasticsearch5(HOSTIP + ':9205')
# es7_client = Elasticsearch(HOSTIP)
mongo_client = pymongo.MongoClient('mongodb://%s:27023/' % HOSTIP)
mssql_client = pymssql.connect(server=HOSTIP, user='sa', password='MSSql-pwd', database=DB)
mysql_client = mysql.connector.connect(host='localhost', user="root", passwd="password", db=DB)

def runworkload(db, optype, ref, conn):
    times = []
    with open(ref,"r+") as file1:
        line_no = 0
        while True:
            line = file1.readline()
            line_no += 1
            print("\r DB = %s | OP = %s | LINE %d" % (db, optype, line_no), end="")
            if line == "":
                break
            words = line.rstrip().split()
            # if db == ELASTIC:
            #     re = elastic7_search(optype, words, conn)
            if db == ELASTIC5:
                re = elastic5_search(optype, words, conn)
            elif db == MONGODB:
                re = mongo_search(optype, words, conn)
            elif db == MSSQL:
                re = mssql_search(optype, words, conn)
            elif db == MYSQL:
                re = mysql_search(optype, words, conn)
            times += [re[1]] 
        print("\r DB = %s | OP = %s | DONE! | Total Queries = %d" % (db, optype, line_no-1), end="")
    mean = sum(times)/len(times)
    return mean*1000

# SRC = "randomwords.txt"
# OPS = [SINGLE, AND, OR]
# DBS = [(MSSQL, mssql_client), (MYSQL, mysql_client), (MONGODB, mongo_client), (ELASTIC5, es5_client)]
SRC = "randomwordstemp.txt"
OPS = [AND]
DBS = [(ELASTIC5, es5_client), (MSSQL, mssql_client), (MYSQL, mysql_client)]

for db in DBS:
    for op in OPS:
        mean = runworkload(db[0], op, SRC, db[1])
        print(" | AvgLatency: %f ms" % mean)