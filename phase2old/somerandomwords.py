from utils import *

randomfile=open("randomwords.txt", "a")
for i in range(0,1000):
    stri = "%s %s %s" % (randomword(4,9), randomword(4,9), randomword(4,9))
    print(stri, file=randomfile)
    print(stri+" | %d items inserted" % (i+1))