"""SimpleApp.py"""

import os
import glob
import shutil
from pyspark import SparkContext

def quiet_logs( sc ):
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


# We create an edge from k to v and from v to k
def bidirectional((k, v)):
    return [(k, v), (v, k)]


# Takes a pair like ("1", ["2","3","4","5"]) and converts it to "1 # 2 3 4 5"
def convert_to_string(key, value):
    return key + " # " + mkstring(value)


def adjacency_list(sc, inputFile, outputFile):
    rdd = initialise(sc, inputFile, parse)
    result = transform(rdd,
                       # The Mapper -> Given a key-value pair like (0,1), it FlatMaps that into [(0,1), (1,0)]
                       bidirectional,
                       # The Reducer -> Given a key-value pair like (0, [1, 2, 3]) it outputs a string to be written in the file: "0 # 1 2 3"
                       lambda (key, value): convert_to_string(key, list(value)))
    finalise(result, outputFile)

input_directory = raw_input("Please enter the directory of files or enter 'default' for the default test cases: ")
if input_directory.lower() == 'default':
    input_directory = "Graph_Input_Files"


# Remove the output directory if it exists, so that the program doesn't crash when saving as text file at the "finalise" method
if os.path.isdir("Ex2_Graph_Conversion.out"):
    shutil.rmtree("Ex2_Graph_Conversion.out")


adjacency_list(sc, input_directory, "Ex2_Graph_Conversion.out")
print "The output can found in Ex2_Graph_Conversion.out"
