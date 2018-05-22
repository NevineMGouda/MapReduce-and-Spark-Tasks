"""SimpleApp.py"""
import os
import shutil
import itertools
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


def combination((k, l)):
    return k, list(itertools.combinations(sorted(l), 2))


def bidirectional((k, v)):
    return [(k, v), (v, k)]


def convert_to_string(a, b):
    return a + " " + str(b)


def triangles(sc, inputFile, outputFile):
    rdd = initialise(sc, inputFile, parse)
    # The Mapper -> Given a key-value pair like (0,1), it FlatMaps that into [(0,1), (1,0)]
    # The Reducer -> Given a key-value pair like (0, [1, 2, 3]) it outputs all the pair combinations betweens all values like [(0, [(1, 2), (1, 3), (2, 3))]
    rdd = transform(rdd, bidirectional, combination)

    rdd = transform(rdd,
                    # The Mapper-> Given an key-value pair input like (0, [(1, 2), (1, 3), (2, 3)] it will flatMap it
                    # and sorts it into [(0, 1, 2), (0, 1, 3), (0, 2, 3)]
                    lambda (k, v): [(tuple(sorted((key, value, k))), 1) for (key, value) in v],
                    # The Reducer-> given a key-value pair produced from a groupByKey, sum the list of values.
                    # For example given ((1, 2, 3), [1, 1]) it will reduce to ((1, 2, 3), 2)
                    lambda (key, values): (key, sum(values)))

    # We filter the keys that their value is not 3. Which means that if ((1,2,3),2) occurs means that between the nodes 1,2 and 3 there is only 2 edges in common
    # For instance an edge between 1,2 and 2,3 but not 1,3 which means that (1,2,3) is not and triangle.
    # Which means that a triangle occurs only with a value == 3
    rdd = rdd.filter(lambda (key, value): value == 3)

    result = transform(rdd,
                    lambda (key, value): [(i, 1) for i in key],
                    lambda (key, values): convert_to_string(key, sum(values)))

    finalise(result, outputFile)

input_directory = raw_input("Please enter the directory of files or enter 'default' for the default test cases: ")
if input_directory.lower() == 'default':
    input_directory = "Graph_Input_Files"

# Remove the output directory if it exists, so that the program doesn't crash when saving as text file at the "finalise" method
if os.path.isdir("Ex4_Triangles.out"):
    shutil.rmtree("Ex4_Triangles.out")


triangles(sc, input_directory, "Ex4_Triangles.out")
print "The output can found in Ex4_Triangles.out"