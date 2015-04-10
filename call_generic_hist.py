from __future__ import division
import scipy.io
import pymongo
from pymongo import MongoClient
from datetime import datetime
from matlab2datetime import matlab2datetime
import numpy as np
import matplotlib.pyplot as plt
import sys
from bson.son import SON

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
    print bins

    if metric is 7:
        times = [matlab2datetime(key) for key in bins.keys()]
        if not times:
            pass
        elif group_time is 0:  # group by hour
            plt.hist([t.hour for t in times], bins = 24) # to bin by hour
            plt.title('Histogram of call times by hour for user #{num}'.format(num=user_num))
            plt.xlabel('Hour')
            plt.show()
        elif group_time is 1:  # group by day of the week
            plt.hist([t.isoweekday() for t in times], bins = 7)
            plt.title('Histogram of call times by day of the week for user #{num}'.format(num=user_num))
            plt.xlabel('Day of the week')
            plt.show()
        elif group_time is 2:  # group by day of the month
            plt.hist([t.day for t in times], bins = 31)
            plt.title('Histogram of call times by day of the month for user #{num}'.format(num=user_num))
            plt.xlabel('Day of the month')
            plt.show()
        else:  # group by month
            plt.hist([t.month for t in times], bins = 12)
            plt.title('Histogram of call times by day for user #{num}'.format(num=user_num))
            plt.xlabel('Month')
            plt.show()
    elif metric is 4:
        if len(bins)<2:  # hist won't work for < 2 data poitns, use bar instead
            plt.bar(bins.keys(), bins.values())
        else:
            bins = {key/60:bins[key] for key in bins.keys()}
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

        plt.bar(range(len(x)), x, align='center')
        plt.xticks(range(len(y)), y, size='small')
        plt.title('Histogram of {m} for user #{num}'.format(m=metric_dict[metric].lstrip('$'), num=user_num))
        plt.xlabel(metric_dict[metric].lstrip('$').capitalize())
        plt.show()

if __name__ == "__main__":
    call_hist(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]))
