#!/usr/bin/env python2.7
#from params import *
from pprint import pprint

#####################################################################
# HOW TO SOLVE:
#
#          Edit this file in gedit
#
#          RUN YOUR CODE using spark-submit solution.py
#          RUN YOUR CODE using spark-submit solution.py
#          RUN YOUR CODE using spark-submit solution.py
#
# 1. Go to the main, you will find the steps you need to solve
# 2. UNCOMMENT the step you need in the main and then scroll up to fill in the corresponding function
# 3. You will find a comment #SOLVE HERE to guide you
#
#   Questions related to printing are solved FOR YOU, just read them.
#
#   DO NOT ADD ANY NEW FUNCTIONS!!!
# ####################################################################


# Step 0 - Create an RDD based on a subset of weblogs (those ending in digit 6)
def step0_loadlogs(sc, logfilepath):
    #DO NOT CHANGE
    logs = sc.textFile(logfilepath)
    return logs #a textfile RDD

def step0_loadaccounts(sc, logfilepath):
    # DO NOT CHANGE
    accounts = sc.textFile(logfilepath)
    return accounts


# Step 1 -  map each request to a pair (userd,1)
def step1(logs):
    # map each request (line) to a pair (userid, 1), then sum the values
    userreqs = logs.map(lambda line: (line.split()[2],1)).reduceByKey(lambda v1,v2:v1+v2)
    return userreqs

# # Step 2 - Show the records for the 10 users with the highest counts
def step2(userreqs):
    freqcount = userreqs.map(lambda (k,v): (v,k)).countByKey()
    return freqcount

# Step 3 - Group IPs by user ID
# return an RDD, mapping each userid to his ips
def step3(logs):
    userips =logs.map(lambda line: (line.split()[2],line.split()[0])).groupByKey().map(lambda x:(x[0],list(x[1])))
    return userips


# Step 4a - Map account data to (userid,[values....])
def step4a(accounts):
    #Solved for you
    useraccounts = accounts.map(lambda s: s.split(',')).map(lambda account: (account[0], account))
    return useraccounts


# Step 4b - Join account data with userreqs then merge hit count into valuelist
def step4b(useraccounts,userreqs):
    accounthits = useraccounts.join(userreqs)
    return accounthits

# Step 4c - Display userid, hit count, first name, last name for the first 5 elements
def step4c(accounthits):
    #solved for you
    for (userid, (values, count)) in accounthits.take(5):
        print  userid, count, values[3], values[4]

# Challenge 1 - key accounts by postal/zip code
def challenge1(accounts):
    accountsByPCode = accounts.keyBy(lambda line: line.split(',')[8])
    return accountsByPCode

# Challenge 2 - map account data to lastname,firstname
def challenge2(accountsByPCode):
    namesByPCode = accountsByPCode.mapValues(lambda v:(v.split(',')[4],v.split(',')[3])).groupByKey().map(lambda (k,v): (k,list(v)))
    return namesByPCode

# Challenge 3 - print the first 5 zip codes and list the names
def challenge3(namesByPCode):
    first5Zips = namesByPCode.sortByKey().take(5)
    for (pcode, names) in first5Zips :
        print "---", pcode
        for name in names:
            print name
    return first5Zips


def print_header(h):
    pprint("=====================================")
    pprint("START: "+h)
    pprint("=====================================")

def print_footer(h):
    pprint("=====================================")
    pprint("END: " + h)
    pprint("=====================================")


def display(header, results):
    print_header(header)
    pprint(results)
    print_footer(header)

def main(sc):

    #--------Step 0.0
    # -------- Step4a (needs step0.1)
    logs = step0_loadlogs(sc, "/loudacre/weblogs/2014-03-15.log")
    display("logs", logs.count())


    #-------- Step1 (needs step0.0)
    userreqs = step1(logs)
    display("userreqs", userreqs.take(2))

    # -------- Step2 (needs step1)
    freqcount = step2(userreqs)
    display("freqcount",freqcount)

    # -------- Step2 (needs step0.0) # solved for you
    userips = step3(logs)
    print_header("userips")
    # print out the first 10 user ids, and their IP list
    for (userid, ips) in userips.take(10):
        print userid, ":"
        for ip in ips: print "\t", ip
    print_footer("userips")

    # -------- Step0.1
    accounts = step0_loadaccounts(sc, "/loudacre/accounts/*")
    display("accounts", accounts.count())

    # -------- Step4a (needs step0.1)
    useraccounts = step4a(accounts)
    display("useraccounts",useraccounts.take(2))

    # -------- Step4b,c (needs step4a, step1)
    accounthits = step4b(useraccounts,userreqs)
    print_header("userips")
    step4c(accounthits) # solved for you
    print_footer("userips")

    # -------- Challenge 1 (needs step0.1)
    accountsByPCode = challenge1(accounts)
    display("accountsByPCode", accountsByPCode.take(2))


    # -------- Challenge 2 (needs challenge1)
    namesByPCode = challenge2(accountsByPCode)
    display("namesByPCode", namesByPCode.take(2))

    # -------- Challenge 2 (needs challenge2)
    challenge3(namesByPCode)

if __name__ == '__main__':
    from pyspark.context import SparkContext
    sc = SparkContext()
    main(sc)
