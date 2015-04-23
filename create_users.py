from __future__ import division
import sys
import pymongo
from datetime import datetime, timedelta
import numpy as np
import random
from bson.son import SON
from scipy import stats
import numpy as np
from collections import Counter


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

num_original_users = 95


def create_users(num_sim_users):

    for user in xrange(num_sim_users):
        print "Input user progress: ", round(user/num_sim_users*100, 1), "%"
        user_num = 100+user

        #select original user at random
        clone_user = random.randint(0, num_original_users-1)
        print "Cloning user #", clone_user

        #Apps
        collection = 'apps'
        pipe = [{"$match": {"user":clone_user}}]
        result = db.command('aggregate', 'apps', pipeline=pipe)
        num_apps = len(result['result'])
        for each_app in xrange(num_apps):
            print "Input app progress: ", round(each_app/num_apps*100, 1), "%"
            app_name = pick_random('apps', 'app_name', clone_user)
            date = pick_time('apps', clone_user, 'year')
            duration = pick_duration_based_on_day('apps', clone_user, date, 'app_date', app_name)
            busy = app_log.find({'user': user_num, 'app_date': {"$gte": date, "$lt": date+timedelta(seconds=duration)}}).count()
            while busy:
                print "Was busy!"
                print "Date: ", date
                print "Duration: ", duration
                print "Current activities: ", busy
                date = date.replace(hour = random.choice([date.hour%23, date.hour%23, date.hour%23, (date.hour+1)%23, (date.hour-1)%23, (date.hour+1)%23, (date.hour-1)%23, (date.hour+2)%23, (date.hour-2)%23]))
                date = date.replace(minute=random.choice(xrange(60)))  # reselect random minute
                busy = app_log.find({'app_date': {"$gte": date, "$lt": date+timedelta(seconds=duration)}}).count()
                duration = max(duration-5, 0)  # decrease duration a little each time we encounter a conflict
            app = {}
            app['user'] = user_num
            app['app_date'] = date
            app['app_duration'] = duration
            app_log.insert(app)

def pick_duration_based_on_day(collection, orig_user_id, date, time_field, app_name):
    hour = date.hour
    dow = date.weekday()
    pipe = [{"$match": {"user":orig_user_id, "app_name": app_name}}]
    result = db.command('aggregate', collection, pipeline=pipe)
    filt_result = []
    for each_bin in result['result']:  # sample from the same hour and dow
        if (each_bin['app_date'].hour == hour) and (each_bin['app_date'].weekday() == dow):
            filt_result.append(each_bin['app_duration'])
    if not filt_result:  # if we can't find any samples for hour AND dow
        for each_bin in result['result']:  # sample from same hour OR dow
            if (each_bin['app_date'].hour == hour) or (each_bin['app_date'].weekday() == dow):
                filt_result.append(each_bin['app_duration'])
    if not filt_result:  # if we can't find any samples from hour OR dow
        for each_bin in result['result']:  # sample from all options
            filt_result.append(each_bin['app_duration'])
    return random.choice(filt_result)

def pick_random(collection, field, orig_user_id):
    pipe = [{"$match": {"user":orig_user_id}}, {"$group": {"_id":"$" +field, "count": {"$sum":1}}}, {"$sort": SON([("count", -1), ("_id", -1)])}]
    result = db.command('aggregate', collection, pipeline=pipe)
    counts = {each_app['_id']:each_app['count'] for each_app in result['result']}
    x = counts.keys()
    x.sort()
    y = [counts[key] for key in x]
    values = [val+1 for val in y]  # smooth
    total = sum(values)
    y = [(val)/total for val in values]
    rand_dist = (np.arange(len(x)), y)
    rv_gen = stats.rv_discrete(values=rand_dist)
    idx = rv_gen.rvs(size=1)[0]
    return x[idx]

def pick_time(collection, orig_user_id, unit_time):
    pipe = [{"$match": {"user":orig_user_id}}]
    result = db.command('aggregate', collection, pipeline=pipe)
    if unit_time is 'year':
        # pick year
        years = [each_app['app_date'].year for each_app in result['result']]
        year = random.choice(years)
        dates = [each_app['app_date'] for each_app in result['result'] if each_app['app_date'].year == year]

        # pick month based on year
        months = [each_app.month for each_app in dates]
        month = random.choice(months)
        dates = [each_app for each_app in dates if each_app.month == month]


        # pick day of month based on month and year
        doms = [each_app.day for each_app in dates]
        dom = random.choice(doms)
        dates = [each_app for each_app in dates if each_app.day == dom]


        # pick hour based on dom, month, and year
        hours = [each_app.hour for each_app in dates]
        hour = random.choice(hours)

        # randomly generate minute
        minute = random.choice(xrange(60))

        # return time as a datetime object
        return datetime(year, month, dom, hour, minute)


if __name__ == "__main__":
    create_users(int(sys.argv[1]))
