from newutils import *

es = Elasticsearch5(HOSTIP + ':9205')
es7 = Elasticsearch(HOSTIP)
client = pymongo.MongoClient('mongodb://%s:27023/' % HOSTIP)
mssql_conn = pymssql.connect(server=HOSTIP, user='sa', password='MSSql-pwd', database=DB)
mysql_connection = mysql.connector.connect(host='localhost', user="root", passwd="password", db=DB)

def runworkload_es5(optype, ref, es):
    times = []
    with open(ref,"r+") as file1:
        while True:
            line = file1.readline()
            if line == "":
                break
            words = line.rstrip().split()
            re = elastic5_search(optype, words, es)
            times += [re[1]] 
    mean = sum(times)/len(times)
    return mean*1000

def runworkload_es7(optype, ref, es):
    times = []
    with open(ref,"r+") as file1:
        while True:
            line = file1.readline()
            if line == "":
                break
            words = line.rstrip().split()
            re = elastic7_search(optype, words, es)
            times += [re[1]] 
    mean = sum(times)/len(times)
    return mean*1000

def runworkload_mongo(optype, ref, es):
    times = []
    with open(ref,"r+") as file1:
        while True:
            line = file1.readline()
            if line == "":
                break
            words = line.rstrip().split()
            re = mongo_search(optype, words, es)
            times += [re[1]] 
    mean = sum(times)/len(times)
    return mean*1000

def runworkload_mssql(optype, ref, es):
    times = []
    with open(ref,"r+") as file1:
        while True:
            line = file1.readline()
            if line == "":
                break
            words = line.rstrip().split()
            re = mssql_search(optype, words, es)
            times += [re[1]] 
    mean = sum(times)/len(times)
    return mean*1000

def runworkload_mysql(optype, ref, es):
    times = []
    with open(ref,"r+") as file1:
        while True:
            line = file1.readline()
            if line == "":
                break
            words = line.rstrip().split()
            re = mysql_search(optype, words, es)
            times += [re[1]] 
    mean = sum(times)/len(times)
    return mean*1000

SRC = "randomwords.txt"

# print("es5 mean for AND: ", runworkload_es5(AND, SRC, es))
# print("es5 mean for OR: ", runworkload_es5(OR, SRC, es))
# print("es5 mean for SINGLE: ", runworkload_es5(SINGLE, SRC, es))

# print("mongo mean for AND: ", runworkload_mongo(AND, SRC, client))
# print("mongo mean for OR: ", runworkload_mongo(OR, SRC, client))
# print("mongo mean for SINGLE: ", runworkload_mongo(SINGLE, SRC, client))

# print("msssql mean for AND: ", runworkload_mssql(AND, SRC, mssql_conn))
# print("mssql mean for OR: ", runworkload_mssql(OR, SRC, mssql_conn))
# print("mssql mean for SINGLE: ", runworkload_mssql(SINGLE, SRC, mssql_conn))

# print("mysql mean for AND: ", runworkload_mysql(AND, SRC, mysql_connection))
# print("mysql mean for OR: ", runworkload_mysql(OR, SRC, mysql_connection))
# print("mysql mean for SINGLE: ", runworkload_mysql(SINGLE, SRC, mysql_connection))

# print("es7 mean for AND: ", runworkload_es7(AND, SRC, es7))
# print("es7 mean for OR: ", runworkload_es7(OR, SRC, es7))
# print("es7 mean for SINGLE: ", runworkload_es7(SINGLE, SRC, es7))

re = elastic5_search(SINGLE, ['data science'], es)