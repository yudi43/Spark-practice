from pyspark import SparkConf, SparkContext
import collections


#writing local means not running on cluster
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)


#the most common way of creating a rdd is using textFile
lines = sc.textFile("file:////home/yudi/workspace/Spark-practice/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()
    print("%s %i" % (key, value))
