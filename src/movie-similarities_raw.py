from __future__ import print_function
import sys
from pyspark import SparkContext, SparkConf
from math import sqrt
import collections

def loadMovieNames():
    movieNames = {}
    with open("/mnt/d/github/spark/SparkCourse1/ml-100k/u.item", encoding='utf8', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def makePairs(userRatings):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return ((movie1, movie2), (rating1, rating2))

def filterDuplicates(userRatings):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2

def computeCosineSimilarity(ratingPairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX*ratingX
        sum_yy += ratingY*ratingY
        sum_xy += ratingX*ratingY
        numPairs += 1
    
    numerator = sum_xy
    denominator = sqrt(sum_xx)*sqrt(sum_yy)
    score = 0
    if (denominator):
        score=(numerator)/float(denominator)
    return (score, numPairs)

# local[*]: sparks built in cluster manager and treat every core on your desktop as a node on a cluster
conf = SparkConf().setMaster("local[*]").setAppName("MovieSimilarities")
sc = SparkContext.getOrCreate(conf=conf)

print("\nMovie Dictionary: ")
nameDict = loadMovieNames()
for key, value in nameDict.items():
    print(key, value)

data = sc.textFile("../ml-100k/u.data")
#map ratings to key/value pairs: userID, MovieID, rating
ratings = data.map(lambda x:x.split()).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))
print("\n rating data (first)")
ratings.foreach(print)
#(846, (1178, 2.0))
#(764, (106, 2.0))
#(486, (546, 2.0))
#(797, (259, 3.0))
#(816, (690, 4.0))
#(788, (443, 4.0))
#permutation (userID, ((movieID, rating), (movieID, rating)))
joinedRatings=ratings.join(ratings)
print("\n Self joined rating data (first)")
print(joinedRatings.take(10))
#[(196, ((242, 3.0), (242, 3.0))), (196, ((242, 3.0), (393, 4.0))), (196, ((242, 3.0), (381, 4.0))), (196, ((242, 3.0), (251, 3.0))), (196, ((242, 3.0), (655, 5.0))), (196, ((242, 3.0), (67, 5.0))), (196, ((242, 3.0), (306, 4.0))), (196, ((242, 3.0), (238, 4.0))), (196, ((242, 3.0), (663, 5.0))), (196, ((242, 3.0), (111, 4.0)))]

uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)
moviePairs = uniqueJoinedRatings.map(makePairs)

print(moviePairs.take(5))
#[((242, 393), (3.0, 4.0)), ((242, 381), (3.0, 4.0)), ((242, 251), (3.0, 3.0)), ((242, 655), (3.0, 5.0)), ((242, 306), (3.0, 4.0))]
moviePairRatings = moviePairs.groupByKey()
print(type(moviePairRatings))
#<class 'pyspark.rdd.PipelinedRDD'>
print(moviePairRatings.take(5))
#[((242, 580), <pyspark.resultiterable.ResultIterable object at 0x7f9e08463430>), ((242, 692), <pyspark.resultiterable.ResultIterable object at 0x7f9e084633a0>), ((242, 428), <pyspark.resultiterable.ResultIterable object at 0x7f9e08463370>), ((242, 340), <pyspark.resultiterable.ResultIterable object at 0x7f9e08463550>), ((393, 1241), <pyspark.resultiterable.ResultIterable object at 0x7f9e08463520>)]
moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()
print(moviePairSimilarities.take(5))
#[((242, 580), (0.9443699330874624, 6)), ((242, 692), (0.9203762039948743, 18)), ((242, 428), (0.9419097988977888, 15)), ((242, 340), (0.9455404837184603, 32)), ((393, 1241), (1.0, 1))]
#moviePairSimilarities.sortByKey()
#moviePairSimilarities.saveAsTextFile("movie-sims")
if (len(sys.argv) > 1):

    scoreThreshold = 0.97
    coOccurenceThreshold = 50

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter(lambda pairSim: \
        (pairSim[0][0] == movieID or pairSim[0][1] == movieID) \
        and pairSim[1][0] > scoreThreshold and pairSim[1][1] > coOccurenceThreshold)

    # Sort by quality score.
    results = filteredResults.map(lambda pairSim: (pairSim[1], pairSim[0])).sortByKey(ascending = False).take(10)

    print("Top 10 similar movies for " + nameDict[movieID])
    for result in results:
        (sim, pair) = result
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = pair[0]
        if (similarMovieID == movieID):
            similarMovieID = pair[1]
        print(nameDict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))

#Top 10 similar movies for Kolya (1996)
#Full Monty, The (1997)  score: 0.9709298420498284       strength: 76
#L.A. Confidential (1997)        score: 0.9704578642283557       strength: 64