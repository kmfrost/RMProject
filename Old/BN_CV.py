from __future__ import division
import sys
import pymongo
from datetime import datetime, timedelta
import numpy as np
import random
from bson.son import SON
from scipy import stats
from collections import Counter
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

db = client.rm_db
app_log = db.apps
call_log = db.calls
cell_data_log = db.cell_data
home_log = db.homes
tower_log = db.locs
place_log = db.places
sms_log = db.sms

num_original_users = 95

test_mode = False
num_test = 50

# End initiailizations
# ------------------------------------------------------------------------------
# Begin main function

def BN_CV():
    # Define BN dependencies
    BN_time = []
    BN_user = []
    BN_phone = ['Time', 'User']
    BN_apps = ['Time', 'User']
    BN_loc = ['Time', 'User']
    BN_sms = ['Time', 'User']
    BN_cell = ['Time', 'User']
    BN_relat = ['Phone']
