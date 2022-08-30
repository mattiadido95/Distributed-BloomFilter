import math
import sys
import time
import  os
from pyspark import SparkContext
from decimal import Decimal, ROUND_HALF_UP
import math
import configparser
p = 0.01
def word_split(line):
    items = line.split("\t")
    return items[0], int(Decimal(items[1]).quantize(0, ROUND_HALF_UP))

def parameters(count_rating):
    n = count_rating[1]
    m = int(- (n * math.log(p)) / (math.pow(math.log(2), 2.0)))
    k = int((m / n) * math.log(2))
    return count_rating[0], n, m, k


if __name__ == '__main__':
    master = "local"
    if len(sys.argv) == 2:
        master = sys.argv[1]
    sc = SparkContext(master, "BoomFilter")

    #stage1
    rdd_input = sc.textFile("data.txt")
    rdd_title_rating = rdd_input.map(word_split)
    count_rating = rdd_title_rating.map(lambda x: (x[1],1)).reduceByKey(lambda x, y: x + y).sortByKey() #(rating, count_rating)
    parameter_rating = count_rating.map(parameters)
    print(parameter_rating.collect())






