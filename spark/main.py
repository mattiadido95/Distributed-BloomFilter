import json
import math
import time
from decimal import Decimal, ROUND_HALF_UP
from pyspark import SparkContext
from model.bloom_filter import BloomFilter
from utils import *

config = 0
with open('config.json') as f:
    config = json.load(f)

p = config['p']


# (map function) from input line get title and rounded rating
def word_split(line):
    items = line.split("\t")
    return items[0], int(Decimal(items[1]).quantize(0, ROUND_HALF_UP))


# (map function) count the number of films for each rating / work on a partition of input
def counter_rating(rows):
    counter = [0 for _ in range(10)] # array[10] of zeros
    for row in rows: # increment the rating counter of partition
        counter[row[1] - 1] = counter[row[1] - 1] + 1
    return zip(range(1, 11), counter)


# (map function) create a bloomfilter for each rating
def bf_creation(rows):
    bloomfilters = [BloomFilter(m, k) for r, n, m, k, in parameter_rating.value]
    for row in rows:  # (title, rating) = row
        bloomfilters[row[1] - 1].add(row[0])
    return zip(range(1, 11), bloomfilters)


# (map function) count the number of false positive for each rating
def bf_compare(rows):
    counter = [0 for _ in range(10)]
    for row in rows:
        for i in range(10):
            if (i + 1) == row[1]:
                continue
            if rdd_bf.value[i][1].find(row[0]):
                counter[i] = counter[i] + 1
    return zip(range(1, 11), counter)


if __name__ == '__main__':
    sc = SparkContext(config['master'], "BloomFilter") # open context (connection) of spark cluster shared in each node
    sc.addPyFile("bloom_filter.zip")

    # stage1

    start1 = time.time()
    rdd_input = sc.textFile(config['input']).filter(lambda kv: kv != "tconst\taverageRating\tnumVotes") # read dataset less first line
    rdd_title_rating = rdd_input.map(word_split) # call map with word_split() that parse input line with round
    count_rating = rdd_title_rating.mapPartitions(counter_rating) # take a portion of input not only one line like in the map() and than compute counter_rating()
    count_rating = count_rating.reduceByKey(lambda x, y: x + y).sortByKey()  # (rating, count_rating)
    param = []
    n_tot = 0
    for x, n in count_rating.collect(): # count for each rating params m , k
        n_tot = n_tot + n
        m = int(- (n * math.log(p)) / (math.pow(math.log(2), 2.0)))
        k = int((m / n) * math.log(2))
        param.append((x, n, m, k))
    parameter_rating = sc.broadcast(param) # put params in broadcast, shared variables in read only mode ( sc rappresent the cluster )
    elapsed_time_stage1 = time.time() - start1

    # stage2

    start2 = time.time()
    rdd_bf = sc.broadcast(
        rdd_title_rating.mapPartitions(bf_creation).reduceByKey(lambda x, y: x.merge(y)).sortByKey().collect())
    parameter_rating.destroy()  # remove the broadcast variable from both executors and driver
    elapsed_time_stage2 = time.time() - start2

    # stage3

    start3 = time.time()
    rdd_compare = rdd_title_rating.mapPartitions(bf_compare).reduceByKey(lambda x, y: x + y).sortByKey()
    rdd_bf.destroy()
    percentage = []
    for rating, false_positive in rdd_compare.collect():
        percentage.append((rating, false_positive / (n_tot - param[rating - 1][1])))
    elapsed_time_stage3 = time.time() - start3

    # write results on file
    smm = elapsed_time_stage3 + elapsed_time_stage2 + elapsed_time_stage1
    write_output(percentage, config['output_file'])
    times = [elapsed_time_stage1, elapsed_time_stage2, elapsed_time_stage3]
    write_duration(p, times, config['stats_file'])
