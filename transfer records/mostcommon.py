from nltk.corpus import stopwords 
from nltk.tokenize import word_tokenize 

example_sent = "This is a sample sentence, showing off the stop words filtration."

stop_words = set(stopwords.words('english')) 

word_tokens = word_tokenize(example_sent) 

filtered_sentence = [w for w in word_tokens if not w in stop_words] 

filtered_sentence = [] 

for w in word_tokens: 
	if w not in stop_words: 
		filtered_sentence.append(w) 

print(word_tokens) 
print(filtered_sentence) 

wordcountdict = {}

def updatedict(ntext):
    global wordcountdict
    stop_words = set(stopwords.words('english')) 
    word_tokens = word_tokenize(ntext) 
    filtered_words = []
    for w in word_tokens:
        w = w.lower()
        if w not in stop_words:
            if w in wordcountdict.keys():
                wordcountdict[w] += 1