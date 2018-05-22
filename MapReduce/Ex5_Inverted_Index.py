"""SimpleApp.py"""

import os
import re
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
    input = sc.wholeTextFiles(inputFile)
    return input.map(prepare)

def finalise(data, outputFile):
    """store data in given file"""
    data.saveAsTextFile(outputFile)


def transform(input, mapper, reducer):
    """map reduce framework"""
    return input.flatMap(mapper).groupByKey().map(reducer)


def convert_to_string(key, value):
    return key + " # " + mkstring(value)


def mkstring(alist):
    """convert a list into a space-separated string"""
    return ' '.join([str(x) for x in alist])


def inverted_index(sc, inputFile, outputFile):
    # We get the directory from the inputFile so that later we remove the whole directory and only leave the file name without the path or extension
    dir = "file:" + os.path.dirname(__file__) + "/" + inputFile + "/"

    # WholeTextFiles is used instead of sc.TextFile in order to have a key value pair where the key is the filename and the value is the whole content of the file
    rdd = sc.wholeTextFiles(inputFile)

    rdd = transform(rdd,
                    # The Mapper-> Given pairs like: (d1, "this is a whole file.") we flatMap that into
                    # [("this", d1), ("is", d1), ("a", d1), ("whole", d1), ("file", d1)] Where we also split on all the listed special characters and space

                    lambda (k, v): [(x.lower(), k[len(dir):].replace(".txt", "")) for x in
                                    re.split(r'\s+|[.,!@#$%^&*\(\)\{\}_+=\\/<>:|\[\]\"\'?;]\s*', v)],

                    # The Reducer -> We first remove duplicate documents by casting list to set, then re cast it to list,
                    # so that we can sort the documents ids in increasing order
                    # And then we convert the key-value pair to a string to string to be written in file like "hello # d1 d2 d3"
                    # Which means that the word hello appears in documents d1, d2 and d3.
                    lambda (k, v): convert_to_string(k, sorted(list(set(v)))))

    result = rdd.filter(lambda x: x[0] != " ")
    finalise(result, outputFile)

# Take the input directory from command line
input_directory = raw_input("Please enter the directory of files or enter 'default' for the default test cases: ")
if input_directory.lower() == 'default':
    input_directory = "Ex5_Input_Files"



# Remove the output directory if it exists, so that the program doesn't crash when saving as text file at the "finalise" method
if os.path.isdir("Ex5_Inverted_Index.out"):
    shutil.rmtree("Ex5_Inverted_Index.out")


# Start the word count after pre-processing
inverted_index(sc, input_directory, "Ex5_Inverted_Index.out")
print "The output can found in Ex5_Inverted_Index.out directory"
