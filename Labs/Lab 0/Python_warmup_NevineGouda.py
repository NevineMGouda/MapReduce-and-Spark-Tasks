def myfreq(events):
    d={}
    for i in range(len(events)):
        if events[i] in d.keys():
            d[events[i]]+=1
        else:
            d[events[i]]=1
    print d
events = [1,2,1,3,1,3,1,2,4,3,6,12,3,4,5,1,2,3,2,5,6,7,8,7,9,1,2,3,4,6]
mmyfreq(events)