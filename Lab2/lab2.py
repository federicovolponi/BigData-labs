### Exercise 2
from pyspark import SparkConf, SparkContext
import time
import sys

start = time.time()

conf = SparkConf().setAppName("My app")
sc = SparkContext(conf = conf)
prefix = sys.argv[1]
outputPath = sys.argv[2]

rdd = sc.textFile("/data/students/bigdata_internet/lab2/word_frequency.tsv")
splitRDD = rdd.map(lambda l: (l.split("\t")[0], int(l.split("\t")[1])))

PrefixWordRDD = splitRDD.filter(lambda word: word[0].startswith(prefix))
toPrintRDD = PrefixWordRDD.map(lambda word: f"{word[0]} - {word[1]},")

toPrintRDD.saveAsTextFile(outputPath)

stop = time.time()
print(f"Program took {stop - start} seconds to run")