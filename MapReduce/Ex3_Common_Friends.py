"""SimpleApp.py"""

import os
import glob
import shutil
import itertools
from pyspark import SparkContext


def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
  logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

sc = SparkContext("local")

quiet_logs(sc)


## MapReduce Framework
def initialise(sc, inputFile, prepare):
    """Open a file and apply the prepare function to each line"""
    input = sc.textFile(inputFile)
    return input.map(prepare)


def finalise(data, outputFile):
    """store data in given file"""
    data.saveAsTextFile(outputFile)


def transform(input, mapper, reducer):
    """map reduce framework"""
    return input.flatMap(mapper).groupByKey().map(reducer)


# utility functions
def parse(ins):
    """converts "(a,b)" into ("a","b")"""
    s = ins.strip()
    result = ("", "")
    if len(s) > 0 and s[0] == '(':
        comma = s.find(',')
        if comma >= 0:
           end = s.find(')', comma)
           if end >= 0:
             result = (s[1: comma].strip(),
                       s[comma+1: end].strip())
    return result


def mkstring(alist):
    """convert a list into a space-separated string"""
    return ' '.join([str(x) for x in alist])


def swap((k, v)):
    return v, k


def combination((k,l)):
    return k, list(itertools.combinations(sorted(l), 2))


def bidirectional((k, v)):
    return [(k, v), (v, k)]


# Takes a pair like ("1", "2") and converts it to "1 2"
def convert_to_string((a, b)):
    return a + " " + b


def common_friends(sc, inputFile, outputFile):
    rdd = initialise(sc, inputFile, parse)
    # The Mapper -> Given a key-value pair like (0,1), it FlatMaps that into [(0,1), (1,0)]
    # The Reducer -> Given a key-value pair like (0, [1, 2, 3]) it outputs all the pair combinations betweens all values like [(0, [(1, 2), (1, 3), (2, 3))]
    rdd = transform(rdd, bidirectional, combination)
    result = transform(rdd,
                       # The Mapper -> Given a key-value pair like [(0, [(1, 2), (1, 3), (2, 3))] it outputs all the pair combinations betweens all values like [((1, 2), 0), ((1, 3), 0), ((2, 3), 0)]
                       lambda (k, v): [(pair, k) for pair in v],
                       # The Reducer -> It transforms the key-value pair after grouping into a string to be written in the file in the format like: 1 2 # 3 4 5
                       lambda (key, value): convert_to_string(key) + " # " + mkstring(value))

    finalise(result, outputFile)

input_directory = raw_input("Please enter the directory of files or enter 'default' for the default test cases: ")
if input_directory.lower() == 'default':
    input_directory = "Graph_Input_Files"

# Remove the output directory if it exists, so that the program doesn't crash when saving as text file at the "finalise" method
if os.path.isdir("Ex3_Common_Friends.out"):
    shutil.rmtree("Ex3_Common_Friends.out")


common_friends(sc, input_directory, "Ex3_Common_Friends.out")
print "The output can found in Ex3_Common_Friends.out"