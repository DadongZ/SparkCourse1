from pyspark import SparkConf, SparkContext
import collections

# configuration
##  master node is the local machine, not cluster
##  job name: RatingsHistogram
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")

# create an spark context object, always call sc
sc = SparkContext.getOrCreate(conf = conf) 

#u.data cols: userid, movieid, rating, timestamp
#create RDD lines using sc.textFile, read line by line
lines = sc.textFile("../ml-100k/u.data")
#RDD.map transform
ratings = lines.map(lambda x: x.split()[2])
#RDD functions -> key, value
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))

for key, value in sortedResults.items():
    print("%s %i" % (key, value))