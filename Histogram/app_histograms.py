from __future__ import division
import sys
sys.path.insert(0, '../')
import scipy.io
import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
from matlab2datetime import matlab2datetime
import numpy as np
import matplotlib.pyplot as plt
import sys
from bson.son import SON
from collections import defaultdict, Counter

def app_hist(user_num, metric, group_time=0):
    #setup the database
    try:
        client = pymongo.MongoClient()
        print "\nConnected to MongoDB\n"
    except pymongo.errors.ConnectionFailure, e:
        print "Could not connect to MongoDB: %s" % e
    db = client.rm_db
    apps = db.apps

    metric_dict={1: "$app_date", 2: "$app_name", 3:"$app_duration"}

    if metric is 1:
        pipe = [{"$match": {"user":user_num}}]
        result = db.command('aggregate', 'apps', pipeline=pipe)

        times = []
        for each_bin in result['result']:
            times.append(each_bin['app_date'])

        if not times:
            pass
        elif group_time is 0:  # group by hour
            width = 1
            hours = [t.hour for t in times]
            bins, values = zip(*Counter(hours).items())
            plt.bar(bins, values, width) # to bin by hour
            plt.title('Histogram of app usage times by hour for user #{num}'.format(num=user_num))
            plt.xlabel('Hour')
            plt.xticks([x + width*0.5 for x in xrange(24)], xrange(24))
            plt.show()
        elif group_time is 1:  # group by day of the week
            weekdays = [t.weekday() for t in times]
            bins, values = zip(*Counter(weekdays).items())
            width = 1
            plt.bar(bins, values, width)
            plt.xlabel('Day of the week')
            plt.xticks([x + width*0.5 for x in bins], ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
            plt.title('Histogram of app usage times by day of the week for user #{num}'.format(num=user_num))
            plt.show()
        elif group_time is 2:  # group by day of the month
            width = 1
            days = [t.day for t in times]
            bins, values = zip(*Counter(days).items())
            plt.bar(bins, values, width)
            plt.title('Histogram of app usage times by day of the month for user #{num}'.format(num=user_num))
            plt.xlabel('Day of the month')
            plt.xticks([x + width*0.5 for x in xrange(1, 32)], xrange(1, 32))
            plt.show()
        else:  # group by month
            months = [t.month for t in times]
            bins, values = zip(*Counter(months).items())
            width = 1
            plt.bar(bins, values, width)
            plt.title('Histogram of tower transitions by month for user #{num}'.format(num=user_num))
            plt.xlabel('Month')
            plt.xticks([x+width*0.5 for x in xrange(1,13)], ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'])
            plt.title('Histogram of app usage times by month for user #{num}'.format(num=user_num))
            plt.show()

    elif metric is 3:
        pipe = [{"$match": {"user":user_num}}]
        result = db.command('aggregate', 'apps', pipeline=pipe)
        bins = defaultdict(list)
        for each_bin in result['result']:
            bins[each_bin['app_name']].append(each_bin['app_duration'])
        averages = {}
        for each_bin in bins:
            avg_secs = sum(bins[each_bin])/len(bins[each_bin])
            averages[each_bin] = avg_secs/60.0  # change to average minutes
        if not averages:
            print "No app duration data for user #{num}\n".format(num=user_num)
        else:
            y = sorted(averages.values())
            x = sorted(averages, key=averages.get)

            plt.bar(range(len(x)), y, align='center')
            plt.xticks(range(len(x)),x, size='small')
            plt.title('Histogram of average app duration times for user #{num}\n'.format(num=user_num))
            plt.xlabel('App Name')
            plt.ylabel('Average time in use (minutes)')
            plt.show()

    else:
        pipe = [{"$match": {"user":user_num}}, {"$group": {"_id":metric_dict[metric], "count": {"$sum":1}}}, {"$sort": SON([("count", -1), ("_id", -1)])}]
        result = db.command('aggregate', 'apps', pipeline=pipe)
        bins = {}
        for each_bin in result['result']:
            bins[each_bin['count']] = each_bin['_id']
        x = bins.keys()
        x.sort()
        y = [bins[key] for key in x]

        plt.bar(range(len(x)), x, align='center')
        plt.xticks(range(len(y)), y, size='small')
        plt.title('Histogram of number of times each app is opened for user #{num}\n'.format(num=user_num))
        plt.xlabel('App Name')
        plt.ylabel('Number of times accessed')
        plt.show()

if __name__ == "__main__":
    app_hist(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]))
