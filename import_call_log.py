import scipy.io
import pymongo

#setup the database
try:
    client = pymongo.MongoClient()
    print "\nConnected to MongoDB\n"
except pymongo.errors.ConnectionFailure, e:
    print "Could not connect to MongoDB: %s" % e
db = client.rm_db
calls = db.calls

mat_data = scipy.io.loadmat('network_data.mat')
comm_data = mat_data['s']

x, num_users = comm_data.shape

for user in xrange(num_users):
    print "\nAdding user #", user
    for idx, each_call in enumerate(comm_data[0][user][0][0]):
        print "Adding call #", idx
        call_log = {}
        call_log['user'] = user
        call_log['date'] = float(each_call[0][0][0])
        call_log['event'] = int(each_call[1][0][0])
        call_log['contact'] = int(each_call[2][0][0])
        call_log['description'] = str(each_call[3][0])
        call_log['direction'] = str(each_call[4][0])
        try:
            call_log['duration'] = int(each_call[5][0][0])
        except IndexError, e:  # missed calls are an empty array
            call_log['duration'] = 0
        call_log['hashNum'] = float(each_call[6][0][0])
        calls.insert(call_log)

calls.find()
