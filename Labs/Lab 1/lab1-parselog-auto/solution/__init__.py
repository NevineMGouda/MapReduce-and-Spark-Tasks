#   Instructions:
#   - This the ONLY file you are going to submit as a solution
#   - It can not be renamed
#   - It assumes that all references to external data files are loaded into the params file
#   - Do not put ANY REFERENCES to data files in this file
#   - Do not remove/modify the from solution import *

from pprint import pprint

LOG_FILE="data/sample.txt"

def parseLog(logFile):
    """
    :param logFile: a web log file to parse where each line looks like this
    199.72.81.55 - - [01/Jul/1995:00:00:01 - 0400] "GET /history/apollo/ HTTP/1.0" 200 6245
    :return: a list of tuples [ ('199.72.81.55', 'GET', '/history/apollo/', 6245), ...] where
    the tuple contains (host, verb,url, time) respectively

    """
    #Hints
    # 0. Create an empty list called for example results
    # 1. use "open" to open the file, and call it f (no need to import anything)
    # 2. use a loop to iterate over the lines
    # 3. use split(x) to break the lines where x is a delimiter
    # 5. use the resulting array after many split operations to finally construct the (host, verb,url, time) tuple
    # 6. time should be returned as int and not a string
    # 7. using results.append to add each tuple to a list
    # 8. close the file when you are done
    # 9. return the results list
    # 10. if all good, run the grademe.py script

    ## SOLVE HERE
    l=[]
    f=open(logFile,"r")
    for i in f:
        item=i.split()
        x=(item[0],item[5][1:],item[6],int(item[9]))
        l.append(x)
    f.close()
    return l

        

def main():
    results = parseLog(LOG_FILE)
    print(results)