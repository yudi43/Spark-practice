#51,3378,19.80
#42,6926,57.77
#2,4424,55.77
#79,9291,33.17
#50,3901,23.57
#20,6633,6.49
#15,6148,65.53

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('SpentByCustomer')
sc = SparkContext(conf = conf)

def extractCustomerPricePairs(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

input = sc.textFile('file:////home/yudi/workspace/Spark-practice/customer-orders.csv')

mappedInput = input.map(extractCustomerPricePairs)

totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)
results = totalByCustomer.collect()
for result in results:
    print(result)
