import sys
sys.path.insert(0, '../')
import scipy.io
import pymongo
from datetime import datetime, timedelta
from matlab2datetime import matlab2datetime

#setup the database
try:
    client = pymongo.MongoClient()
    print "\nConnected to MongoDB\n"
except pymongo.errors.ConnectionFailure, e:
    print "Could not connect to MongoDB: %s" % e
db = client.rm_orig_db
places = db.places

mat_data = scipy.io.loadmat('network_data.mat')
comm_data = mat_data['s']

x, num_users = comm_data.shape

for user in xrange(num_users):
    print "\nAdding user #", user
    if comm_data[0][user][40]:
        struct = comm_data[0][user][40][0][0]
        startdate = matlab2datetime(struct[5][0][0]) + timedelta(hours=4)
        days = 0
        hours = struct[7]
        hour = int(hours[0][0])
        locations = struct[10]
        for idx in xrange(len(hours)):
            try:
                location = int(locations[idx][0])
            except ValueError:
                location = 0
            if location is 1:
                location_label = 'Home'
            elif location is 2:
                location_label = 'Work'
            elif location is 3:
                location_label = 'Elsewhere'
            else:
                location_label = 'No signal'
            place = {}
            place['user'] = user
            place['date'] = startdate + timedelta(days=days) + timedelta(hours=hour)
            place['place'] = location_label
            place['startdate'] = startdate
            place['num_hours'] = len(hours)
            place['end_date'] = matlab2datetime(struct[6][0][0]) + timedelta(hours=4)
            places.insert(place)
            hour += 1
            if hour is 24:
                days += 1
                hour = 0