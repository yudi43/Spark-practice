from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('WordCount')

sc = SparkContext(conf = conf)

input = sc.textFile('file:////home/yudi/workspace/Spark-practice/Book')
words = input.flatMap(lambda x: x.split())

#create a dictionary which contains the number of each occurance for every word...
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord, count)




