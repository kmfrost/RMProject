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
sms = db.sms

mat_data = scipy.io.loadmat('network_data.mat')
comm_data = mat_data['s']

x, num_users = comm_data.shape

for user in xrange(num_users):
    print "\nAdding user #", user
    for idx, each_sms in enumerate(comm_data[0][user][20]):
        print "Adding call #", idx
        sms_log = {}
        sms_log['user'] = user
        sms_log['date'] = float(each_sms[0])
        sms.insert(sms_log)
