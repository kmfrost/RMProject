import sys
sys.path.insert(0, '../')
import scipy.io
import pymongo
from matlab2datetime import matlab2datetime
import datetime

#setup the database
try:
    client = pymongo.MongoClient()
    print "\nConnected to MongoDB\n"
except pymongo.errors.ConnectionFailure, e:
    print "Could not connect to MongoDB: %s" % e
db = client.rm_db
apps = db.apps

mat_data = scipy.io.loadmat('network_data.mat')
comm_data = mat_data['s']

x, num_users = comm_data.shape

for user in xrange(num_users):
    print "\nAdding user #", user
    num_apps = len(comm_data[0][user][16][0])
    for idx in xrange(num_apps):
        print "Adding app #", idx
        app = {}
        app['user'] = user
        app['app_date'] = matlab2datetime((comm_data[0][user][18][0][idx]))
        try:
            app['app_name'] = str(comm_data[0][user][16][0][idx][0])
        except UnicodeEncodeError:
            print "\n\n ERROR OCCURED.  Adding ", comm_data[0][user][16][0][idx]
            app['app_name'] = str(comm_data[0][user][16][0][idx])

        if idx<num_apps-1:
            time_to_next_app = abs(matlab2datetime(comm_data[0][user][18][0][idx+1]) - app['app_date']).seconds
            app['app_duration']=min(time_to_next_app, 1800)  # if they're using an app for more than 30 minutes, assume the phone was just left in that state
        else:
            app['app_duration'] = datetime.timedelta(minutes=5).seconds
        apps.insert(app)
