from __future__ import division
import sys
sys.path.insert(0, '../')
import scipy.io
import pymongo
from pymongo import MongoClient
from datetime import datetime
from matlab2datetime import matlab2datetime
import numpy as np
import matplotlib.pyplot as plt
import sys
from bson.son import SON
from collections import Counter

def call_hist(user_num, metric, group_time=0):
    #setup the database
    try:
        client = pymongo.MongoClient()
        print "\nConnected to MongoDB\n"
    except pymongo.errors.ConnectionFailure, e:
        print "Could not connect to MongoDB: %s" % e
    db = client.rm_db
    calls = db.calls

    metric_dict={1: "$contact", 2: "$description", 3: "$direction", 4: "$duration", 5: "$event", 6: "$hashNum", 7: "$date"}
    pipe = [{"$match": {"user":user_num}}, {"$group": {"_id":metric_dict[metric], "count": {"$sum":1}}}, {"$sort": SON([("_id", -1)])}]
    result = db.command('aggregate', 'calls', pipeline=pipe)

    bins = {}
    for each_bin in result['result']:
        bins[each_bin['_id']] = each_bin['count']

    if metric is 7:
        times = [matlab2datetime(key) for key in bins.keys()]
        if not times:
            pass
        elif group_time is 0:  # group by hour
            width = 1
            hours = [t.hour for t in times]
            bins, values = zip(*Counter(hours).items())
            values = [val+1 for val in values]  # smooth
            total = sum(values)
            values = [(val)/total for val in values]
            plt.bar(bins, values, width) # to bin by hour
            plt.xticks([x + width*0.5 for x in xrange(24)], xrange(24))
            plt.title('Histogram of call times by hour for user #{num}'.format(num=user_num))
            plt.xlabel('Hour')
            plt.show()
        elif group_time is 1:  # group by day of the week
            weekdays = [t.weekday() for t in times]
            bins, values = zip(*Counter(weekdays).items())
            width = 1
            values = [val+1 for val in values]  # smooth
            total = sum(values)
            values = [(val)/total for val in values]
            plt.bar(bins, values, width)
            plt.xlabel('Day of the week')
            plt.xticks([x + width*0.5 for x in bins], ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
            plt.title('Histogram of call times by day of the week for user #{num}'.format(num=user_num))
            plt.show()
        elif group_time is 2:  # group by day of the month
            width = 1
            days = [t.day for t in times]
            bins, values = zip(*Counter(days).items())
            values = [val+1 for val in values]  # smooth
            total = sum(values)
            values = [(val)/total for val in values]
            plt.bar(bins, values, width)
            plt.xticks([x + width*0.5 for x in xrange(1, 32)], xrange(1, 32))
            plt.title('Histogram of call times by day of the month for user #{num}'.format(num=user_num))
            plt.xlabel('Day of the month')
            plt.show()
        else:  # group by month
            months = [t.month for t in times]
            bins, values = zip(*Counter(months).items())
            values = [val+1 for val in values]  # smooth
            total = sum(values)
            values = [(val)/total for val in values]
            width = 1
            plt.bar(bins, values, width)
            plt.xticks([x+width*0.5 for x in xrange(1,13)], ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'])
            plt.title('Histogram of call times by day for user #{num}'.format(num=user_num))
            plt.xlabel('Month')
            plt.show()
    elif metric is 4:
        if not bins:
            print "No phone log for user #{num}\n".format(num=user_num)
        elif len(bins)<2:  # hist won't work for < 2 data poitns, use bar instead
            bins = {key/60:bins[key]+1 for key in bins.keys()}
            total = sum(bins.values())
            bins = {key:bins[key]/total for key in bins.keys()}
            
            plt.bar(bins.keys(), bins.values())
            plt.title('Histogram of call durations for user #{num}'.format(num=user_num))
            plt.xlabel('Call duration in minutes')
            plt.show()
        else:
            bins = {key/60:bins[key]+1 for key in bins.keys()}
            total = sum(bins.values())
            bins = {key:bins[key]/total for key in bins.keys()}
          #  bins = {pow(key,0.25)/60:bins[key] for key in bins.keys()}
            bin_arr = np.array(bins.items())
            bin_arr = bin_arr[np.argsort(bin_arr[:,0])]
            x, weights = bin_arr.T
            plt.hist(x, weights=weights)
            plt.title('Histogram of call durations for user #{num}'.format(num=user_num))
            plt.xlabel('Call duration in minutes')
            plt.show()

    else:
        y = bins.keys()
        y.sort()
        x = [bins[key] for key in y]
        values = [val+1 for val in x]  # smooth
        total = sum(values)
        x = [(val)/total for val in values]

        plt.bar(xrange(len(x)), x, align='center')
        plt.xticks(xrange(len(y)), y, size='small')
        plt.title('Histogram of call {m} for user #{num}'.format(m=metric_dict[metric].lstrip('$'), num=user_num))
        plt.xlabel(metric_dict[metric].lstrip('$').capitalize())
        plt.show()

if __name__ == "__main__":
    call_hist(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]))
