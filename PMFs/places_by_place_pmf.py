from __future__ import division
import sys
sys.path.insert(0, '../')
from collections import Counter
import scipy.io
import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
from matlab2datetime import matlab2datetime
import numpy as np
import matplotlib.pyplot as plt
import sys
from bson.son import SON

def place_hist(user_num, metric, group_time=0, group_place=1):
    #setup the database
    try:
        client = pymongo.MongoClient()
        print "\nConnected to MongoDB\n"
    except pymongo.errors.ConnectionFailure, e:
        print "Could not connect to MongoDB: %s" % e
    db = client.rm_db
    places = db.places

    metric_dict={1: "$date", 2: "$place"}
    place_dict = {1: "Home", 2: "Work", 3: "Elsewhere", 4: "No signal"}
    place = place_dict[group_place]
    print place
    if metric is 1:
        pipe = [{"$match": {"user":user_num, 'place':place}}]
        result = db.command('aggregate', 'places', pipeline=pipe)
        
        pipe2 = [{"$match": {"user":user_num, 'place':{"$nin": [place, place_dict[4]]}}}]
        result2 = db.command('aggregate', 'places', pipeline=pipe2)
        
        times = []
        for each_bin in result['result']:
            times.append(each_bin['date'])
            
        not_times = []
        for each_bin in result2['result']:
            not_times.append(each_bin['date'])
        

        if not times:
            print "No place data available for user #", user_num, "\n"
        elif group_time is 0:  # group by hour
            hours = [t.hour for t in times]
            not_hours = [t.hour for t in not_times]
            bins, values = zip(*Counter(hours).items()) 
            not_bins, not_values = zip(*Counter(not_hours).items())   
            values = [value/(not_values[idx]+value) for idx, value in enumerate(values) if bins[idx]==not_bins[idx]]                
            width = 1
            plt.bar(bins, values, width)
            plt.title('Probability that user #{num} is at {place} by hour'.format(num=user_num, place=place))
            plt.xticks([x + width*0.5 for x in xrange(24)], xrange(24))
            plt.xlabel('Hour')
            plt.show()
        elif group_time is 1:  # group by day of the week
            weekdays = [t.weekday() for t in times]
            bins, values = zip(*Counter(weekdays).items())
            width = 1
            values = [val for val in values]  # smooth
            total = sum(values)
            values = [(val)/total for val in values]            
            plt.bar(bins, values, width)
            plt.xlabel('Day of the week')
            plt.xticks([x+width*0.5 for x in bins], ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
            plt.title('Histogram of locations by day of the week for user #{num}'.format(num=user_num))
            plt.show()
        elif group_time is 2:  # group by day of the month
            width = 1
            days = [t.day for t in times]
            bins, values = zip(*Counter(days).items())
            values = [val for val in values]  # smooth
            total = sum(values)
            values = [(val)/total for val in values]            
            plt.bar(bins, values, width)
            plt.xticks([x + width*0.5 for x in xrange(1, 32)], xrange(1, 32))
            plt.title('Histogram of locations by day of the month for user #{num}'.format(num=user_num))
            plt.xlabel('Day of the month')
            plt.show()
        else:  # group by month
            months = [t.month for t in times]
            bins, values = zip(*Counter(months).items())
            width = 1
            values = [val for val in values]  # smooth
            total = sum(values)
            values = [(val)/total for val in values]            
            plt.bar(bins, values, width)
            plt.title('PMF of locations by month for user #{num}'.format(num=user_num))
            plt.xlabel('Month')
            plt.xticks([x+1+width*0.5 for x in xrange(12)], ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'])
            plt.title('PMF of locations by month for user #{num}'.format(num=user_num))
            plt.show()

    else:
        pipe = [{"$match": {"user":user_num}}, {"$group": {"_id":metric_dict[metric], "count": {"$sum":1}}}, {"$sort": SON([("count", -1), ("_id", -1)])}]
        result = db.command('aggregate', 'places', pipeline=pipe)

        if not result['result']:
            print "No place data available for user #{num}\n".format(num=user_num)
        else:
            bins = {}
            for each_bin in result['result']:
                bins[each_bin['count']] = each_bin['_id']
            x = bins.keys()
            x.sort()
            y = [bins[key] for key in x]
            values = [val+1 for val in x]  # smooth
            total = sum(values)
            x = [(val)/total for val in values]

            plt.bar(range(len(x)), x, align='center')
            plt.xticks(range(len(y)), y, size='small')
            plt.title('PMF of places visited by user #{num}'.format(num=user_num))
            plt.xlabel('Place')
            plt.ylabel('Number of times visited')
            plt.show()

if __name__ == "__main__":
    place_hist(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]))
