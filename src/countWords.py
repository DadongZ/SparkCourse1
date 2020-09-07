from pyspark import SparkContext, SparkConf
import collections
import re #regular expression

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster('local').setAppName('CountWords')
sc=SparkContext.getOrCreate(conf=conf)

lines=sc.textFile('/mnt/d/github/spark/SparkCourse1/SparkCourse/book.txt')
words = lines.flatMap(normalizeWords)
wordsCount = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y)
wordsCountSorted = wordsCount.map(lambda x: (x[1],x[0])).sortByKey()

results = wordsCountSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word, count, sep='\t') 