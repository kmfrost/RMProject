import scipy.io
import pymongo
from pymongo import MongoClient
from datetime import datetime

#setup the database
client = MongoClient()
db = client.rm_database
users = db.users

mat_data = scipy.io.loadmat('network_data.mat')
comm_data = mat_data['s']

x, num_users = comm_data.shape
user_call_hists = {}
user_call_directions = []
for user in xrange(3):
    direction = []

    for each_call in comm_data[0][user][0][0]:
        direction.append(each_call[4][0])

    direction_hist = dict((x, direction.count(x)) for x in direction)
    user_call_directions.append(direction)
    user_call_hists[user] = direction_hist

print user_call_hists
