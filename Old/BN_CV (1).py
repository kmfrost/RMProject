from __future__ import division
import sys
import pymongo
from datetime import datetime, timedelta
import numpy as np
import random
from bson.son import SON
from scipy import stats
from collections import Counter, defaultdict
import time

# End imports
# ------------------------------------------------------------------------------
# Begin initializations

#setup the database
try:
    client = pymongo.MongoClient()
    print "\nConnected to MongoDB\n"
except pymongo.errors.ConnectionFailure, e:
    print "Could not connect to MongoDB: %s" % e

db = client.rm_orig_db
app_log = db.apps
call_log = db.calls
cell_log = db.cell_data
home_log = db.homes
tower_log = db.locs
place_log = db.places
sms_log = db.sms

num_users = 95

num_cv_test = 20

# End initiailizations
# ------------------------------------------------------------------------------
# Begin main function

def BN_CV():
    # Define BN dependencies
    BN_1 = {}
    BN_1['Time'] = []
    BN_1['User'] = []

    BN_2 = {}
    BN_2['Calls'] = ['Time', 'User']
    BN_2['Apps'] = ['Time', 'User']
    BN_2['Places']= ['Time', 'User']
    BN_2['SMS']= ['Time', 'User']
    BN_2['Cell']= ['Time', 'User']

    BN_3 = {}
    BN_3['Calls'] = ['contact', 'date', 'description', 'direction', 'duration']
    BN_3['Apps'] = ['app_date', 'app_duration', 'app_name']
    BN_3['Places'] = ['place', 'date']
    BN_3['SMS'] = ['date']
    BN_3['Cell'] = ['date']

    time_dict = ['day', 'weekday', 'hour']

    BN_2_accuracy = defaultdict(list)

    #go through each user:
    for user in xrange(num_users):
        try:
            places = place_log.find({'user': user})[0]
        except IndexError:  # no place info for this user
            continue  # skip this user and go onto the next
        num_hours = places['num_hours']
        if num_hours < 168:
            continue
        start_date = places['startdate']
        cv_start = start_date + timedelta(hours=num_hours*0.6)  # start CV data 60% of the way in
        test_start = start_date + timedelta(hours=num_hours*0.8)  # start test data 80% of the way in

        for test_num in xrange(num_cv_test):
            predict_field = random.choice(BN.keys)
            conditioned_on = 'Time'

            for sort_time in time_dict:
                if sort_time is 'day':
                    pass
                elif sort_time is 'weekday':
                    pass
                else:  # sort_time is 'hour':
