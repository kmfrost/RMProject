import scipy.io
import pymongo
from pymongo import MongoClient
from datetime import datetime
from matlab2datetime import matlab2datetime
import numpy as np
import matplotlib.pyplot as plt
import sys

def call_direction_hist(user_num):
    #setup the database
    try:
        client = pymongo.MongoClient()
        print "\nConnected to MongoDB\n"
    except pymongo.errors.ConnectionFailure, e:
        print "Could not connect to MongoDB: %s" % e
    db = client.rm_db
    calls = db.calls

    pipe = [{"$match": {"user":user_num}}, {"$group": {"_id":"$direction", "count": {"$sum":1}}}]
    result = calls.aggregate(pipe)

    bins = {}
    for each_bin in result['result']:
        bins[each_bin['_id']] = each_bin['count']

    plt.bar(range(len(bins.values())), bins.values(), align='center')
    plt.xticks(range(len(bins.keys())), bins.keys(), size='small')
    plt.show()

if __name__ == "__main__":
    call_direction_hist(int(sys.argv[1]))
