import math
import sys
import time
import os
from pyspark import SparkContext
from decimal import Decimal, ROUND_HALF_UP
import math
import configparser
from model.bloom_filter import BloomFilter

p = 0.01


def word_split(line):
    items = line.split("\t")
    return items[0], int(Decimal(items[1]).quantize(0, ROUND_HALF_UP))

def counter_rating(rows):
    counter = [0 for _ in range(10)]
    for row in rows:
        counter[row[1]-1] = counter[row[1]-1] + 1
    rating = range(1, 11)
    return zip(rating, counter)

def parameters(count_rating):
    n = count_rating[1]
    m = int(- (n * math.log(p)) / (math.pow(math.log(2), 2.0)))
    k = int((m / n) * math.log(2))
    return count_rating[0], n, m, k


def bf_creation(rows):
    bloomfilters = [BloomFilter(m, k) for r, n, m, k, in parameter_rating.value]
    for row in rows:  # (title, rating) = row
        bloomfilters[row[1] - 1].add(row[0])

    rating = range(1, 11)
    return zip(rating, bloomfilters)


def bf_compare(rows):
    counter = [0 for _ in range(10)]
    for row in rows:
        for i in range(10):
            if (i+1) == row[1]:
                continue
            if(rdd_bf.value[i][1].find(row[0])):
                counter[i] = counter[i] + 1
    rating = range(1, 11)
    return zip(rating, counter)



def ciao(x,y):
    return x[1] + y[1]

if __name__ == '__main__':
    master = "yarn"
    if len(sys.argv) == 2:
        master = sys.argv[1]


    sc = SparkContext(master, "BloomFilter")
    sc.addPyFile("bloom_filter.zip")

    # stage1
    start1 = time.time()

    # rdd_input = sc.textFile("data.tsv").zipWithIndex().filter(lambda kv: kv[1] > 0).keys()
    rdd_input = sc.textFile("data.tsv").filter(lambda kv: kv != "tconst\taverageRating\tnumVotes")
    rdd_title_rating = rdd_input.map(word_split)
    count_rating = rdd_title_rating.mapPartitions(counter_rating)
    count_rating = count_rating.reduceByKey(lambda x , y : x + y).sortByKey()# (rating, count_rating)
    #parameter_rating = sc.broadcast(count_rating.map(parameters).collect())
    param = []
    ntot= 0
    for x,n in count_rating.collect():
        ntot = ntot + n
        m = int(- (n * math.log(p)) / (math.pow(math.log(2), 2.0)))
        k = int((m / n) * math.log(2))
        param.append((x, n, m, k))
    parameter_rating = sc.broadcast(param)

    elapsed_time_stage1 = time.time() - start1


    # stage2
    start2 = time.time()
    rdd_bf = sc.broadcast(rdd_title_rating.mapPartitions(bf_creation).reduceByKey(lambda x,y : x.or_b2b(y)).sortByKey().collect())
    #parameter_rating.destroy() #remove the broadcast variable from both executors and driver
    #for x,y in rdd_bf.collect():
    #    print(str(x) +" "+ str(y.get()))
    elapsed_time_stage2 = time.time() - start2

    #stage3
    start3 = time.time()
    rdd_compare = rdd_title_rating.mapPartitions(bf_compare).reduceByKey(lambda x,y : x + y).sortByKey()
    percentage = []
    for rating, false_positive in rdd_compare.collect():
        percentage.append((rating,false_positive/ (ntot-param[rating-1][1])))
    elapsed_time_stage3 = time.time() - start3

    with open("stage-durationSpark.txt", "a") as f:
        f.write(str(elapsed_time_stage1) + "\n")
        f.write(str(elapsed_time_stage2) + "\n")
        f.write(str(elapsed_time_stage3) + "\n")
        f.write("------ end execution ------\n")

    for x, y in percentage:
        print(str(x) + "\t" + str(y))


