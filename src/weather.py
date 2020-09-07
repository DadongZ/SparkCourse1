from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster('local').setAppName('weather')
sc = SparkContext.getOrCreate(conf=conf)

def parseLine(line):
    fields = line.split(',')
    stationID=fields[0]
    entryType=fields[2]
    temperature=float(fields[3])*0.1*(9.0/5.0)+32.0
    return (stationID, entryType, temperature)

# weatherstationid, date, max/min, temparature, etc.
lines = sc.textFile("../SparkCourse/1800.csv")
rdd = lines.map(parseLine)
rdd = rdd.filter(lambda x: 'TMIN' in x[1])
rdd = rdd.map(lambda x: (x[0],x[2])) 
minTemps = rdd.reduceByKey(lambda x,y: min(x,y))

res = minTemps.collect()

for item in res:
    print(item[0]+"\t{:.2f}F".format(item[1]))