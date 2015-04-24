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
import math


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
        print "Input user progress: {prog}/{tot} = {perc}%".format(prog=user, tot=num_sim_users, perc=round(user/num_sim_users*100, 2))
        user_num = 100+user

        #select original user at random
        clone_user = random.randint(0, num_original_users-1)
        print "Cloning user #", clone_user
        add_apps(clone_user, user_num)
        add_calls(clone_user, user_num)
        add_cell_data(clone_user, user_num)
        
        #add homeID
        towerID = home_log.find({'user':clone_user})[0]['towerID']
        home_log.insert({'user':user_num, 'towerID':towerID})
        
        add_locations(clone_user, user_num)


def add_apps(clone_user, user_num):
        #Apps
        prog_count = 0
        collection = 'apps'
        pipe = [{"$match": {"user":clone_user}}]
        result = db.command('aggregate', 'apps', pipeline=pipe)['result']
        num_apps = len(result)
        
        app_names = pick_random('apps', 'app_name', clone_user, num_apps)
        durations = pick_app_duration('apps', clone_user, 'app_date', Counter(app_names))

        #sort durations so the biggest are first
        durations = sorted(durations, key=lambda x: x[1])
        durations.reverse()
        
        for app_name, duration in durations:    
            print "App input progress: {prog}/{tot} = {perc}%".format(prog=prog_count, tot=num_apps, perc=round(prog_count/num_apps*100, 2))
        
            date = pick_time('apps', clone_user, 'year', 'app_name', app_name)
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
            prog_count += 1
            
def add_calls(clone_user, user_num):
        #Calls
        prog_count = 0
        collection = 'calls'
        pipe = [{"$match": {"user":clone_user}}]
        result = db.command('aggregate', collection, pipeline=pipe)['result']
        num_calls = len(result)
        num_calls = 10
        
        contact = -1
        hashNum = float('nan')
        event = -1
        
        durations = pick_duration(result, clone_user, 'date', num_calls)
        durations.sort(reverse=True)
        dates = pick_times(result, clone_user, 'date', 'year', num_calls)
        
        for duration, date in zip(durations, dates): 
            print duration, "ASDF", date   
            print "Call input progress: {prog}/{tot} = {perc}%".format(prog=prog_count, tot=num_calls, perc=round(prog_count/num_calls*100, 2))
            call = {}
            call['user'] = user_num
            call['event'] = event
            call['contact'] = contact
            call['duration'] = duration
            call['date'] = date

            direction, description = pick_call_direction_description(clone_user, date, result)
            busy = call_log.find({'user': user_num, 'date': {"$gte": date, "$lt": date+timedelta(seconds=duration)}}).count()
            while busy:
                print "Was busy!"
                print "Date: ", date
                print "Duration: ", duration
                print "Current activities: ", busy
                date = date.replace(hour = random.choice([date.hour%23, date.hour%23, date.hour%23, (date.hour+1)%23, (date.hour-1)%23, (date.hour+1)%23, (date.hour-1)%23, (date.hour+2)%23, (date.hour-2)%23]))
                date = date.replace(minute=random.choice(xrange(60)))  # reselect random minute
                busy = call_log.find({'date': {"$gte": date, "$lt": date+timedelta(seconds=duration)}}).count()
                duration = max(duration-5, 0)  # decrease duration a little each time we encounter a conflict

            call['direction'] = direction
            call['description'] = description
            calls.insert(call)
            prog_count += 1            

def add_cell_data(clone_user, user_num):
    #Cell Data
    prog_count = 0
    collection = 'cell_data'
    pipe = [{"$match": {"user":clone_user}}]
    result = db.command('aggregate', collection, pipeline=pipe)['result']
    num_cell_data = len(result)
    
    dates = pick_times(result, clone_user, 'date', 'year', num_cell_data)
    for date in dates:
        data_log = {}
        data_log['user'] = user_num
        data_log['date'] = date
        cell.insert(data_log)
        
def add_locations(clone_user, user_num):
    #Locations (Cell tower pings)
    prog_count = 0
    collection = 'locs'
    pipe = [{"$match": {"user":clone_user}}]
    result = db.command('aggregate', collection, pipeline=pipe)['result']
    num_locs = len(result)
    
    dates = pick_times(result, clone_user, 'time', 'year', num_locs)
    for date in dates:
        data_log = {}
        data_log['user'] = user_num
        data_log['date'] = date
        cell.insert(data_log)
    

def pick_app_duration(collection, orig_user_id, time_field, app_dict):
    durations = []
    for key in app_dict.keys():
        pipe = [{"$match": {"user":orig_user_id, "app_name": key}}]
        result = db.command('aggregate', collection, pipeline=pipe)
        filt_result = [each_bin['app_duration'] for each_bin in result['result']]

        for each_app in xrange(app_dict[key]):
            durations.append((key, random.choice(filt_result)))
    
    return durations

def pick_call_direction_description(orig_user_id, date, result):
    # try to condition on hour of day and DOW
    filt_result = [(each_bin['direction'], each_bin['description']) for each_bin in result if (each_bin['date'].hour==date.hour and each_bin['date'].weekday()==date.weekday())]
    if not filt_result:  # if that returns nothing
        #filter by hour of the day only
        filt_result = [(each_bin['direction'], each_bin['description']) for each_bin in result if (each_bin['date'].hour==date.hour)]
    directions, descriptions = zip(*filt_result)
    return random.choice(directions), random.choice(descriptions)

def pick_duration(result, orig_user_id, time_field, num_events):
    filt_result = [each_bin['duration'] for each_bin in result]
    durations = [random.choice(filt_result) for each_event in xrange(num_events)]
    return durations

def pick_random(collection, field, orig_user_id, num):
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
    rand_idx = rv_gen.rvs(size=num)
    rand_out = [x[idx] for idx in rand_idx]
    return rand_out

def pick_time(collection, orig_user_id, unit_time, field, filt_on):
    pipe = [{"$match": {"user":orig_user_id, field: filt_on}}]
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

def pick_times(result, orig_user_id, time_field, unit_time, num_events):
    times = []
    if unit_time is 'year':
        for each_event in xrange(num_events):
            
            # pick year
            years = [each_time[time_field].year for each_time in result]
            year = random.choice(years)
            dates = [each_time[time_field] for each_time in result if each_time[time_field].year == year]

            # pick month based on year
            months = [each_time.month for each_time in dates]
            month = random.choice(months)
            dates = [each_time for each_time in dates if each_time.month == month]


            # pick day of month based on month and year
            doms = [each_time.day for each_time in dates]
            dom = random.choice(doms)
            dates = [each_time for each_time in dates if each_time.day == dom]


            # pick hour based on dom, month, and year
            hours = [each_time.hour for each_time in dates]
            hour = random.choice(hours)

            # randomly generate minute
            minute = random.choice(xrange(60))

            # return time as a datetime object
            times.append(datetime(year, month, dom, hour, minute))
    return times

if __name__ == "__main__":
    create_users(int(sys.argv[1]))
