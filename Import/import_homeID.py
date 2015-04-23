import sys
sys.path.insert(0, '../')
import scipy.io
import pymongo

#setup the database
try:
    client = pymongo.MongoClient()
    print "\nConnected to MongoDB\n"
except pymongo.errors.ConnectionFailure, e:
    print "Could not connect to MongoDB: %s" % e
db = client.rm_db
homes = db.homes

mat_data = scipy.io.loadmat('network_data.mat')
comm_data = mat_data['s']

x, num_users = comm_data.shape

for user in xrange(num_users):
    print "\nAdding user #", user
    home = {}
    home['user'] = user
    try:
        home['towerID'] = comm_data[0][user][34][0][0]
    except IndexError, e:  # some home locations were not supported
        home['towerID'] = -1
    homes.insert(home)
