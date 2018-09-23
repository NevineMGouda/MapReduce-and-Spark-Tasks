def ch10(logfile):
     #load an rdd contianing the log file here
    logs = sc.textFile(logfile)
    htmllogs  = logs.filter(lambda line: ".html" in line)
    users    =  htmllogs.map(lambda line: line.split()[0]+"/"+line.split()[2])
    return (logs,htmllogs, users)
