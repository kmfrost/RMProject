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
cell = db.cell_data

mat_data = scipy.io.loadmat('network_data.mat')
comm_data = mat_data['s']

x, num_users = comm_data.shape

for user in xrange(num_users):
    print "\nAdding user #", user
    for idx, each_call in enumerate(comm_data[0][user][24]):
        print "Adding call #", idx
        data_log = {}
        data_log['user'] = user
        data_log['date'] = float(each_call[0])
        cell.insert(data_log)
