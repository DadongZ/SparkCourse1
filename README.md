# Big Data with Apache Spark and Python

[![Course](https://www.udemy.com/staticx/udemy/images/v6/logo-coral-light.svg)](https://www.udemy.com/course/taming-big-data-with-apache-spark-hands-on)

## Sections

- [Set up](#set-up)
- [Course materials](#course-materials)
- [Test run](#test-run)
- [Introduction to Spark](#introduction-to-spark)
- [RDD](#rdd)
- [Run Spark on Cluster](#run-spark-on-cluster)


### Set up
[Follow this page for setting up spark-python](https://sundog-education.com/spark-python/)

[Apache Spark Installation](https://www.tutorialspoint.com/apache_spark/apache_spark_installation.htm)

[Anaconda 3](https://docs.anaconda.com/anaconda/install/)

### Course materials
[Python Script](http://media.sundog-soft.com/Udemy/SparkCourse.zip)

[Movie data](https://grouplens.org/datasets/movielens/)

### Test run
```bash
./run
```

### Introduction to Spark
<img src="./figs/spark.png" width="400">

- A fast and general engine for large-scale data processing
- 100x faster than Hadoop MapReduce in memory
- DAG engine optimizes workflows
- Code in Python, Java, Scala
- Built around the Resilient Distributed Dataset (RDD)


<img src="./figs/components.png" width="400">

### RDD

- Resilient Distributed Dataset (RDD)
- Can create from JSON, CSV, sequence files, object files, compressed files JDBC etc.
- nums = parallelize([1,2,3,4])
- sc.textFile("s3n://path/file/"), hdfs://, file://
- hiveCtx=hiveContext(sc) rows=hiveCtx.sql("SELECT name, age, FROM users")
- operations: map, flatmap, filter, distinct, sample, union, intersection, subtract, cartesian
- functions: collect, count, countByValue, take, top, reduce ...
- SQL-style: join, rightOuterJoin, leftOuterJoin, cogroup, substractByKey
- groupByKey, combineByKey, lookup
- rdd=sc.parallelize([1,2,3,4]) -> rdd.map(lambda(x: x*x)) ->1,4,9,16
- rdd.take(n), rdd.cache()

### Run Spark on Cluster

- Set up AWS EC2 instance
- Partitioning `RDD.partionBy()`
    - trade off between #partitions vs #results
    - At least as many partitions as you have cores
    - partitionBy(100) is usually a reasonable place to start for large operations.
- cp data and script to s3
- sign up EMR and create EMR cluster
- connect EMR by ssh
- spark-submit -execute-memory 1g s3://dz33/spark/aws/movie-similarity_1m.py 260
