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

nameDict = loadMovieNames()

data = sc.textFile("/mnt/d/github/spark/SparkCourse1/ml-100k/u.data")
#map ratings to key/value pairs: userID, MovieID, rating
ratings = data.map(lambda x:x.split()).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))
#permutation (userID, ((movieID, rating), (movieID, rating)))
joinedRatings=ratings.join(ratings)
uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)
moviePairs = uniqueJoinedRatings.map(makePairs)
moviePairRatings = moviePairs.groupByKey()
moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()
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