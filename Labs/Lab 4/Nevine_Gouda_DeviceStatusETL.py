 Example data:
# 2014-03-15:10:10:33,Ronin S2,1a7eca8d-60c9-4d25-8609-d6cfd1ac80a1,0,24,82,72,enabled,enabled,enabled,41,62,36.49259162,-121.003629078
# 2014-03-15:10:10:33/Titanic 2300/d86dbb9d-ff3c-40c6-8685-01f1fac45d9f/59/83/9/3/28/0/enabled/disabled/enabled/34.3456792864/-117.768326105

# Load the data file
devstatus = sc.textFile("/loudacre/devicestatus.txt")

# Filter out lines with < 10 characters, use the 20th character as the delimiter, parse the line, and filter out bad lines
#hint: filter, map, filter
cleanstatus=devstatus.filter(lambda line: len(line)>10).map(lambda line:line.split(line[19])).filter(lambda line: len(line)==14)


# Create a new RDD containing date, manufacturer, device ID, latitude and longitude
devicedata = cleanstatus.map(lambda el: [el[0],el[1].split()[0],el[2],el[12],el[13]])


# Save to a CSV file as a comma-delimited string (trim parenthesis from tuple toString)
output = devicedata.map(lambda line:",".join(line)).saveAsTextFile("/loudacre/devicestatus_etl")

