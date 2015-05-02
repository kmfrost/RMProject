#remove simulated users

import sys
import pymongo


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

for col in [app_log, call_log, cell_data_log, home_log, tower_log, place_log, sms_log]:
    col.remove({'user': {"$gte": 99}})
