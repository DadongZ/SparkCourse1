from pyspark.sql import SparkSession, Row
import collections

spark = SparkSession.builder.appName('SparkSQL').getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=fields[1].encode("utf-8"), age=int(fields[2]), numberFriends=int(fields[3]))

lines = spark.sparkContext.textFile("/mnt/d/github/spark/SparkCourse1/SparkCourse/fakefriends.csv")
people = lines.map(mapper)
print("The top 5 rows of people is %s" %people.take(5))

schemaPeople = spark.createDataFrame(people).cache()
print("The type of schemaPeople is %s" %(type(schemaPeople)))

schemaPeople.createOrReplaceGlobalTempView('people')

teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

schemaPeople.groupBy('age').count().orderBy("age").show()