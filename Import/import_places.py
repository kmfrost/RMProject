import sys
sys.path.insert(0, '../')
import scipy.io
import pymongo
from datetime import datetime


#setup the database
try:
    client = pymongo.MongoClient()
    print "\nConnected to MongoDB\n"
except pymongo.errors.ConnectionFailure, e:
    print "Could not connect to MongoDB: %s" % e
db = client.rm_db
places = db.places

mat_data = scipy.io.loadmat('network_data.mat')
comm_data = mat_data['s']

x, num_users = comm_data.shape

for user in xrange(num_users):
    print "\nAdding user #", user
    if comm_data[0][user][40]:
        struct = comm_data[0][user][40][0][0]
        startdate = datetime.fromordinal(int(struct[5][0][0]))
        days = 0
        hours = struct[7]
        locations = struct[10]
        for idx in xrange(len(hours)):
            hour = int(hours[idx][0])
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
            place['date'] = startdate + datetime.timedelta(days=days) + datetime.timedelta(hours=hour)
            place['place'] = location_label
            place['startdate'] = startdate
            place['num_hours'] = len(hours)
            places.insert(place)
            if hour is 23:
                days += 1
