#  Find K Means of Loudacre device status locations
# 
# Input data: file(s) with device status data (delimited by '|')
# including latitude (13th field) and longitude (14th field) of device locations 
# (lat,lon of 0,0 indicates unknown location)
#
# NOTE: Copy to pyspark using %paste

# for a point p and an array of points, return the index in the array of the point closest to p
def closestPoint(p, points):
    bestIndex = 0
    closest = float("+inf")
    # for each point in the array, calculate the distance to the test point, then return
    # the index of the array point with the smallest distance
    for i in range(len(points)):
        dist = distanceSquared(p,points[i])
        if dist < closest:
            closest = dist
            besssssstIndex = i
    return bestIndex   
# The squared distances between two points
def distanceSquared(p1,p2):  
    return (p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2

# The sum of two points
def addPoints(p1,p2):
    return [p1[0] + p2[0], p1[1] + p2[1]]

# The files with device status data
filename = "/loudacre/devicestatus_etl/*"
    
# K is the number of means (center points of clusters) to find
K = 5

# ConvergeDist -- the threshold "distance" between iterations at which we decide we are done
convergeDist = .1
        
# Parse device status records into [latitude,longitude]
# Filter out records where lat/long is unavailable -- ie: 0/0 points
logs = sc.textFile(filename)
data = logs.map(lambda x: (float(x.split(',')[-2]),float(x.split(',')[-1]))).filter(lambda (x,y): x!=0 and y!=0).persist()
# start with K randomly selected points from the dataset
kpoints = data.takeSample(False,K,42)
# loop until the total distance between one iteration's points and the next is less than the convergence distance specified
tempDist = float("+inf")
while tempDist > convergeDist:
	
    # for each point, find the index of the closest kpoint.  map to (index, (point,1))
    indices = data.map(lambda pair: (closestPoint(pair,kpoints),(pair,1)))
    # For each key (k-point index), reduce by adding the coordinates and number of points
    key_sums = indices.reduceByKey(lambda ((a1,b1),c1),((a2,b2),c2):((a1+a2,b1+b2),c1+c2))
    # For each key (k-point index), find a new point by calculating the average of each closest point
    new_centers = key_sums.map(lambda (index,((x,y),n)):(index,(x/n,y/n))).collectAsMap()
    # calculate the total of the distance between the current points and new points
    total = 0
    for key in new_centers:
	total += distanceSquared(new_centers[key],kpoints[key])
    tempDist = total
    # Copy the new points to the kPoints array for the next iteration
    kpoints = new_centers        
# Display the final points
print kpoints
