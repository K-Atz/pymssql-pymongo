from newutils import *

randomfile=open("randomwords.txt", "w")
randompfile=open("randomphrase.txt", "w")
for i in range(0,1000):
    stri = "%s %s %s" % (randomword(4,9), randomword(4,9), randomword(4,9))
    print(stri, file=randomfile)
    stri = "%s" % randomphrase(4,9)
    print(stri, file=randompfile)
    print("\r%d rows inserted in both files . . ." % (i+1), end="")