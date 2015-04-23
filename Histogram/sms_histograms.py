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

def cell_hist(user_num, group_time=0):
    #setup the database
    try:
        client = pymongo.MongoClient()
        print "\nConnected to MongoDB\n"
    except pymongo.errors.ConnectionFailure, e:
        print "Could not connect to MongoDB: %s" % e
    db = client.rm_db
    sms = db.sms

    pipe = [{"$match": {"user":user_num}}]
    result = db.command('aggregate', 'sms', pipeline=pipe)

    times = []
    for each_bin in result['result']:
        times.append(matlab2datetime(each_bin['date']))

    times.sort()

    if not times:
        print "Did not use SMS.\n"
    elif group_time is 0:  # group by hour
        width = 1
        hours = [t.hour for t in times]
        bins, values = zip(*Counter(hours).items())
        plt.bar(bins, values, width)
        plt.xticks([x + width*0.5 for x in xrange(24)], xrange(24))
        plt.title('Histogram of SMS send/recieve times by hour for user #{num}'.format(num=user_num))
        plt.xlabel('Hour')
        plt.show()
    elif group_time is 1:  # group by day of the week
        weekdays = [t.weekday() for t in times]
        bins, values = zip(*Counter(weekdays).items())
        width = 1
        plt.bar(bins, values, width)
        plt.title('Histogram of SMS send/recieve times by day of the week for user #{num}'.format(num=user_num))
        plt.xlabel('Day of the week')
        plt.xticks([x+width*0.5 for x in bins], ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
        plt.show()
    elif group_time is 2:  # group by day of the month
        plt.hist([t.day for t in times], bins = 31)
        plt.title('Histogram of SMS send/recieve times by day of the month for user #{num}'.format(num=user_num))
        plt.xlabel('Day of the month')
        plt.show()
    else:  # group by month
        months = [t.month for t in times]
        bins, values = zip(*Counter(months).items())
        width = 1
        plt.bar(bins, values, width)
        plt.title('Histogram of SMS send/recieve times by day for user #{num}'.format(num=user_num))
        plt.xlabel('Month')
        plt.xticks([x+1+width*0.5 for x in xrange(12)], ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'])
        plt.show()


if __name__ == "__main__":
    cell_hist(int(sys.argv[1]), int(sys.argv[2]))
