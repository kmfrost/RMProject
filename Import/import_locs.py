import sys
sys.path.insert(0, '../')
from matlab2datetime import matlab2datetime
import scipy.io
import pymongo

#setup the database
try:
    client = pymongo.MongoClient()
    print "\nConnected to MongoDB\n"
except pymongo.errors.ConnectionFailure, e:
    print "Could not connect to MongoDB: %s" % e
db = client.rm_db
locs = db.locs

mat_data = scipy.io.loadmat('network_data.mat')
comm_data = mat_data['s']

x, num_users = comm_data.shape

for user in xrange(num_users):
    print "\nAdding user #", user
    for idx, each_loc in enumerate(comm_data[0][user][5]):
        print "Adding loc #", idx
        loc = {}
        loc['user'] = user
        loc['time'] = matlab2datetime(each_loc[0])
        loc['towerID'] = each_loc[1]
        locs.insert(loc)
