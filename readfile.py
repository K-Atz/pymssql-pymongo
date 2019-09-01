with open("randomwords.txt","r+") as file1:
    while True:
        line = file1.readline()
        if line == "":
            break
        words = line.rstrip().split()
        print(words[0], words[1], words[2])

