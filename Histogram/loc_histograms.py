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

def call_hist(user_num, metric, group_time=0):
    #setup the database
    try:
        client = pymongo.MongoClient()
        print "\nConnected to MongoDB\n"
    except pymongo.errors.ConnectionFailure, e:
        print "Could not connect to MongoDB: %s" % e
    db = client.rm_db
    locs = db.locs
    homes = db.homes

    metric_dict={1: "$time", 2: "$towerID"}
    pipe = [{"$match": {"user":user_num}}, {"$group": {"_id":metric_dict[metric], "count": {"$sum":1}}}, {"$sort": SON([("_id", -1)])}]
    result = db.command('aggregate', 'locs', pipeline=pipe)

    bins = {}
    for each_bin in result['result']:
        bins[each_bin['_id']] = each_bin['count']

    if metric is 1:
        times = [matlab2datetime(key) for key in bins.keys()]
        if not times:
            pass
        elif group_time is 0:  # group by hour
            plt.hist([t.hour for t in times], bins = 24) # to bin by hour
            plt.title('Histogram of tower transitions by hour for user #{num}'.format(num=user_num))
            plt.xlabel('Hour')
            plt.show()
        elif group_time is 1:  # group by day of the week
            weekdays = [t.weekday() for t in times]
            bins, values = zip(*Counter(weekdays).items())
            width = 1
            plt.bar(bins, values, width)
            plt.xlabel('Day of the week')
            plt.xticks([x+width*0.5 for x in bins], ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
            plt.title('Histogram of tower transitions by day of the week for user #{num}'.format(num=user_num))
            plt.show()
        elif group_time is 2:  # group by day of the month
            plt.hist([t.day for t in times], bins = 31)
            plt.title('Histogram of tower transitions by day of the month for user #{num}'.format(num=user_num))
            plt.xlabel('Day of the month')
            plt.show()
        else:  # group by month
            months = [t.month for t in times]
            bins, values = zip(*Counter(months).items())
            width = 1
            plt.bar(bins, values, width)
            plt.title('Histogram of tower transitions by month for user #{num}'.format(num=user_num))
            plt.xlabel('Month')
            plt.xticks([x+1+width*0.5 for x in xrange(12)], ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'])
            plt.show()
    else:
        y = bins.keys()
        y.sort()
        x = [bins[key] for key in y]

        pipe2 = [{"$match": {"user":user_num}}, {"$group": {"_id":"$towerID"}}, {"$sort": SON([("_id", -1)])}]
        result2 = db.command('aggregate', 'homes', pipeline=pipe2)
        homeID = result2['result'][0]['_id']
        print "HomeID: ", homeID

        barlist = plt.bar(range(len(x)), x, align='center')
        if homeID in y:
            home_idx = y.index(homeID)
            barlist[home_idx].set_color('m')
        plt.title('Histogram of tower transitions for user #{num}'.format(m=metric_dict[metric].lstrip('$'), num=user_num))
        plt.xlabel(metric_dict[metric].lstrip('$').capitalize())
        plt.show()

if __name__ == "__main__":
    call_hist(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]))
