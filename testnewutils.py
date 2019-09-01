from newutils import *

es = Elasticsearch5(HOSTIP + ':9205')
client = pymongo.MongoClient('mongodb://%s:27023/' % HOSTIP)

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

print(runworkload_es5(AND, "randomwordstemp.txt", es))
# print(runworkload_mongo(AND, "randomwordstemp.txt", client))

