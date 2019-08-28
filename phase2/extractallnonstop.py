from utils import *

# word1 = 'data'
# word2 = 'computer'
# word3 = 'performance'
# word4 = 'effect'
# word5 = 'job'
# word6 = 'computinh'
# word7 = 'value'

words = [randomword(3), randomword(4), randomword(5)]
dbs = [ELASTIC, MSSQL, MYSQL, MONGODB]
ops = [SINGLE, AND, OR]

for op in ops:
    print(op)
    for db in dbs:
        r = search_words(db, op, words)
        s = ''
        if db == MSSQL:
            s = '\t'
        else:
            s = '\t\t'
        print ("%s%s%s\t%s" %(db, s, r[0], r[1]))
    print("\n")
    words = [randomword(3), randomword(4), randomword(5)]

# for item in search_words(MSSQL, AND, words)[0]:
#     print(item)
# print(search_words(MSSQL, AND, words)[1])


# for op in ops:
#     for db in dbs:
#         r = search_words(db, op, words)
#         print(r[1], r[2])
#     print("\n")
        

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


# r = mongo_search(AND, [word1, word2])
# r = mongo_search(SINGLE, [word1])
# rr = copy.copy(r)
# print("total mongo hits: ", len(list(r)))
# for item in rr:
#     print(item)
#     break

# # r = elastic_search(AND, [word1, word2], COLUMN)
# r = elastic_search(SINGLE, [word1], COLUMN)
# print("total es hits: ",r['hits']['total'])