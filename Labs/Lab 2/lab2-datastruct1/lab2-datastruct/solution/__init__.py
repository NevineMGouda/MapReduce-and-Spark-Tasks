#   Instructions:
#   - This the ONLY file you are going to submit as a solution
#   - It can not be renamed
#   - It assumes that all references to external data files are loaded into the params file
#   - Do not put ANY REFERENCES to data files in this file
#   - Do not remove/modify the from solution import *

from pprint import pprint

def tupleRecordsToDictRecords(records,labels):
    """
    Converts a list of tuples into a list of dictionaries

    :param records: list of tuples, e.g. [("Mahmoud", 21, "student"), ("Heba", 34, "teacher")]
    :param labels: list of strings, e.g. ["name", "age", "job"]
    :return: a list of dictionaries where the keys of each dictionary are the labels in respective order,
    e.g.[{"name":"Mahmoud", "age":21, "job":"student"},{"name":"Heba", "age":34, "job":"teacher"}]
    """
    L=[]
    for i in records:
        d={}
        for j in range(len(labels)):

            d[labels[j]]=i[j]
        L.append(d)
    return L        


def keyByIndex(records, i):
    """
    Given a list of tuples [(1,4,5), (3,5,18), (20,1,8), ...], produce a list of (k,v) tuples
    [   (4, (1,4,5)),
        (5,(3,5,18)),
        (1,(20,1,8)),
    ...], where the keys are the respective fields chosen by the index i in each tuple.
    In the above example the key is 1
    """
    L=[]
    for j in range(len(records)):
        L.append((records[j][i],records[j]))
    return L
        
    


def keyByKey(records, k):
    """
    Given a list of dictionaries, e.g.
    [
       {"name":"Mahmoud", "age":21, "job":"student"},
       {"name":"Heba", "age":34, "job":"teacher"}
     ],
      produce a list of (k,v) tuples
    [
     ("student",{"name":"Mahmoud", "age":21, "job":"student"}),
     ("teacher", {"name":"Heba", "age":34, "job":"teacher"}
    ]
    where the keys are respective fields chosen by the key k in each dictionary.
    In the above example the key is "job"
    """
    L=[]
    for i in range(len(records)):
        L.append((records[i][k],records[i]))
    return L
        



def countByKey(records):
    """
    Given a list or tuples, where the 1st element in the tuple is considered a key, returns
    the count of records having the same key.
    e.g: [("a", 3), ("b", 7), ("a", [1,2,4])] should return {"a":2, "b":1}

    """
    d={}
    for i in range(len(records)):
        key=records[i][0]
        if key in d:
            d[key]+=1
        else:
            d[key]=1
    return d
    


def groupByKey(records):
    '''
    Given a list or tuples, where the 1st element in the tuple is considered a key, groups
    all elements who have the same key, e.g:
    [("a", 3), ("b", 7), ("a", [1,2,4])]
    should return
    {"a":[3,[1,2,4]], "b":[7]}
    '''
    d={}
    for i in range(len(records)):
        key=records[i][0]
        if key in d:
            d[key].append(records[i][1])
        else:
            L=[]
            L.append(records[i][1])
            d[key]=L
    return d    
    

def main():
    print("Running the solution...")
    #put calls to your funcitons to test them here
    #print tupleRecordsToDictRecords([("Mahmoud", 21, "student"), ("Heba", 34, "teacher")],["name", "age", "job"])
    #print keyByIndex([(1,4,5), (3,5,18), (20,1,8)],1)
    #print keyByKey([{"name":"Mahmoud", "age":21, "job":"student"},{"name":"Heba", "age":34, "job":"teacher"}], "job")
    #print countByKey([("a", 3), ("b", 7), ("a", [1,2,4])])  
    #print groupByKey([("a", 3), ("b", 7), ("a", [1,2,4])])
    