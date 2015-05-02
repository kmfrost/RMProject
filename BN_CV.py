from __future__ import division
import sys
import pymongo
from datetime import datetime, timedelta
import numpy as np
import random
from bson.son import SON
from scipy import stats
from collections import Counter, defaultdict
import time

# End imports
# ------------------------------------------------------------------------------
# Begin initializations

#setup the database
try:
    client = pymongo.MongoClient()
    print "\nConnected to MongoDB\n"
except pymongo.errors.ConnectionFailure, e:
    print "Could not connect to MongoDB: %s" % e

db = client.rm_orig_db
app_log = db.apps
call_log = db.calls
cell_log = db.cell_data
home_log = db.homes
tower_log = db.locs
place_log = db.places
sms_log = db.sms

num_users = 95

num_cv_test = 100

# End initiailizations
# ------------------------------------------------------------------------------
# Begin main function

def BN_CV():
    # Define BN dependencies
    BN_1 = {}
    BN_1['Time'] = []
    BN_1['User'] = []

    BN_2 = {}
    BN_2['apps'] = ['Time', 'User']
    BN_2['calls'] = ['Time', 'User']
    BN_2['cell']= ['Time', 'User']
    BN_2['places']= ['Time', 'User']
    BN_2['sms']= ['Time', 'User']

    BN_3 = {}
    BN_3['apps'] = ['date', 'duration', 'name']
    BN_3['calls'] = ['contact', 'date', 'description', 'direction', 'duration']
    BN_3['cell'] = ['date']
    BN_3['places'] = ['place', 'date']
    BN_3['sms'] = ['date']

    BN2_accuracy = defaultdict(list)
    user_accuracy = defaultdict(list)
    TF_app_accuracy = {'TP':0, 'TN':0, 'FP':0, 'FN':0}
    TF_call_accuracy = {'TP':0, 'TN':0, 'FP':0, 'FN':0}
    TF_cell_accuracy = {'TP':0, 'TN':0, 'FP':0, 'FN':0}
    TF_sms_accuracy = {'TP':0, 'TN':0, 'FP':0, 'FN':0}


    #go through each user:
    for user in xrange(num_users):
        try:
            places = place_log.find({'user': user})[0]
        except IndexError:  # no place info for this user
            continue  # skip this user and go onto the next
        num_hours = places['num_hours']
        if num_hours < 1440:  # only look at users who have at least a 2 month's worth of data
            continue
        print "Testing User #{num}".format(num=user)
        start_date = places['startdate']
        cv_start = start_date + timedelta(hours=num_hours*0.6)  # start CV data 60% of the way in
        test_start = start_date + timedelta(hours=num_hours*0.8)  # start test data 80% of the way in
        
        cv_time = test_start - cv_start
        cv_days, cv_seconds = cv_time.days, cv_time.seconds
        cv_hours = int(cv_days*24 + cv_seconds / 3600)
        
        (cv_apps, pos_prior_apps, neg_prior_apps, pos_likelihood_apps_h,
        neg_likelihood_apps_h, cv_calls, pos_prior_calls, neg_prior_calls,
        pos_likelihood_calls_h, neg_likelihood_calls_h, cv_cell, pos_prior_cell,
        neg_prior_cell, pos_likelihood_cell_h, neg_likelihood_cell_h, cv_sms,
        pos_prior_sms, neg_prior_sms, pos_likelihood_sms_h,
        neg_likelihood_sms_h) = binary_classifier_hour(user, num_hours, start_date,
                                                cv_start, test_start)

        (pos_likelihood_apps_dow, neg_likelihood_apps_dow, 
        pos_likelihood_calls_dow, neg_likelihood_calls_dow,
        pos_likelihood_cell_dow, neg_likelihood_cell_dow,
        pos_likelihood_sms_dow,
        neg_likelihood_sms_dow) = binary_classifier_dow(user, num_hours,
                                                        start_date,
                                                        cv_start, test_start)
 
        pipe = [{"$match": {"user":user, "date": {"$lt": cv_start}}}, {"$group": {"_id":"$place", "count": {"$sum":1}}}, {"$sort": SON([("count", -1), ("_id", -1)])}]
        result = db.command('aggregate', 'places', pipeline=pipe)['result']
        places_priors = {'Home':0, 'Work': 0, 'Elsewhere':0, 'No signal': 0}
        total_places = sum([each['count'] for each in result])
        for each in result:
            places_priors[each['_id']] = each['count']/total_places     
        
        (home_likelihood_h, work_likelihood_h, else_likelihood_h,
         no_sig_likelihood_h) = calc_place_likelihoods_hour(user, cv_start)                                   

        (home_likelihood_dow, work_likelihood_dow, else_likelihood_dow,
         no_sig_likelihood_dow) = calc_place_likelihoods_dow(user, cv_start)                                   
        
        
        for test_num in xrange(num_cv_test):
            predict_field = random.choice(BN_2.keys())
            conditioned_on = 'Time'
            
            # sort time by hour:
            rand_hour = random.choice(xrange(cv_hours))
            test_time = cv_start + timedelta(hours=rand_hour)
            test_hour = test_time.hour
            test_dow = test_time.weekday()
            next_time = test_time + timedelta(hours = 1)
            
            
            if predict_field is 'apps':
                pos = pos_prior_apps * pos_likelihood_apps_h[test_hour] * pos_likelihood_apps_dow[test_dow]
                neg = neg_prior_apps * neg_likelihood_apps_h[test_hour] * neg_likelihood_apps_dow[test_dow]
                
                if pos > neg:
                    predict = 1
                else:
                    predict = 0
                cv_apps_hour = [app for app in cv_apps if app > test_time and app < next_time]
                
                if cv_apps_hour and predict:
                    TF_app_accuracy['TP'] += 1
                    BN2_accuracy[predict_field].append(1)
                    user_accuracy[user].append(1)
                elif cv_apps_hour and not predict:
                    TF_app_accuracy['FN'] += 1
                    BN2_accuracy[predict_field].append(0)
                    user_accuracy[user].append(0)
                elif not cv_apps_hour and predict:
                    TF_app_accuracy['FP'] += 1
                    BN2_accuracy[predict_field].append(0)
                    user_accuracy[user].append(0)                    
                else:  # not cv_apps_hour and not predict:
                    TF_app_accuracy['TN'] += 1
                    BN2_accuracy[predict_field].append(1)
                    user_accuracy[user].append(1)
                    
                
            elif predict_field is 'calls':
                pos = pos_prior_calls * pos_likelihood_calls_h[test_hour] * pos_likelihood_calls_dow[test_dow]
                neg = neg_prior_calls * neg_likelihood_calls_h[test_hour] * neg_likelihood_calls_dow[test_dow]
                
                if pos > neg:
                    predict = 1
                else:
                    predict = 0
                cv_calls_hour = [call for call in cv_calls if call > test_time and call < next_time]
                
                if cv_calls_hour and predict:
                    TF_call_accuracy['TP'] += 1
                    BN2_accuracy[predict_field].append(1)
                    user_accuracy[user].append(1)
                elif cv_calls_hour and not predict:
                    TF_call_accuracy['FN'] += 1
                    BN2_accuracy[predict_field].append(0)
                    user_accuracy[user].append(0)                    
                elif not cv_calls_hour and predict:
                    TF_call_accuracy['FP'] += 1
                    BN2_accuracy[predict_field].append(0)
                    user_accuracy[user].append(0)                    
                else:  # not cv_calls_hour and not predict:
                    TF_call_accuracy['TN'] += 1
                    BN2_accuracy[predict_field].append(1)
                    user_accuracy[user].append(1)
            
            elif predict_field is 'cell':
                pos = pos_prior_cell * pos_likelihood_cell_h[test_hour] * pos_likelihood_cell_dow[test_dow]
                neg = neg_prior_cell * neg_likelihood_cell_h[test_hour] * neg_likelihood_cell_dow[test_dow]
                
                if pos > neg:
                    predict = 1
                else:
                    predict = 0
                cv_cell_hour = [cell for cell in cv_cell if cell > test_time and cell < next_time]
                
                if cv_cell_hour and predict:
                    TF_cell_accuracy['TP'] += 1
                    BN2_accuracy[predict_field].append(1)
                    user_accuracy[user].append(1)
                elif cv_cell_hour and not predict:
                    TF_cell_accuracy['FN'] += 1
                    BN2_accuracy[predict_field].append(0)
                    user_accuracy[user].append(0)                    
                elif not cv_cell_hour and predict:
                    TF_cell_accuracy['FP'] += 1
                    BN2_accuracy[predict_field].append(0)
                    user_accuracy[user].append(0)                    
                else:  # not cv_cell_hour and not predict:
                    TF_cell_accuracy['TN'] += 1
                    BN2_accuracy[predict_field].append(1)
                    user_accuracy[user].append(1)
                
            elif predict_field is 'places':
                home = places_priors['Home']*home_likelihood_h[test_hour]*home_likelihood_dow[test_dow]
                work = places_priors['Work']*work_likelihood_h[test_hour]*work_likelihood_dow[test_dow]
                elsewhere = places_priors['Elsewhere']*else_likelihood_h[test_hour]*else_likelihood_dow[test_dow]
                no_signal = places_priors['No signal']*no_sig_likelihood_h[test_hour]*no_sig_likelihood_dow[test_dow]
                
                probs = [home, work, elsewhere, no_signal]
                place_dict = {0: u'Home', 1: u'Work', 2: u'Elsewhere', 3: u'No signal'}
                predict = place_dict[probs.index(max(probs))]

                pipe = [{"$match": {"user":user, "date": {"$lt": next_time, "$gte": test_time}}}]
                result = db.command('aggregate', 'places', pipeline=pipe)['result'][0]
                place = result['place']
                home = places_priors['Home']*home_likelihood_h[test_hour]*home_likelihood_dow[test_dow]
                work = places_priors['Work']*work_likelihood_h[test_hour]*work_likelihood_dow[test_dow]
                elsewhere = places_priors['Elsewhere']*else_likelihood_h[test_hour]*else_likelihood_dow[test_dow]
                no_signal = places_priors['No signal']*no_sig_likelihood_h[test_hour]*no_sig_likelihood_dow[test_dow]
                
                probs = [home, work, elsewhere, no_signal]
                place_dict = {0: u'Home', 1: u'Work', 2: u'Elsewhere', 3: u'No signal'}
                predict = place_dict[probs.index(max(probs))]

                pipe = [{"$match": {"user":user, "date": {"$lt": next_time, "$gte": test_time}}}]
                result = db.command('aggregate', 'places', pipeline=pipe)['result'][0]
                place = result['place']
                
                if place == predict:
                    BN2_accuracy[predict_field].append(1)
                    user_accuracy[user].append(1)                    
                else:
                    BN2_accuracy[predict_field].append(0)                
                    user_accuracy[user].append(0)                    
                
            else:  # predict sms
                pos = pos_prior_sms * pos_likelihood_sms_h[test_hour] * pos_likelihood_sms_dow[test_dow]
                neg = neg_prior_sms * neg_likelihood_sms_h[test_hour] * neg_likelihood_sms_dow[test_dow]
            
                if pos > neg:
                    predict = 1
                else:
                    predict = 0
                cv_sms_hour = [text for text in cv_sms if text > test_time and text < next_time]
            
                if cv_sms_hour and predict:
                    TF_sms_accuracy['TP'] += 1
                    BN2_accuracy[predict_field].append(1)
                    user_accuracy[user].append(1)                    
                elif cv_sms_hour and not predict:
                    TF_sms_accuracy['FN'] += 1
                    BN2_accuracy[predict_field].append(0)
                    user_accuracy[user].append(0)                    
                elif not cv_sms_hour and predict:
                    TF_sms_accuracy['FP'] += 1
                    BN2_accuracy[predict_field].append(0)
                    user_accuracy[user].append(0)                    
                else:  # not cv_sms_hour and not predict:
                    TF_sms_accuracy['TN'] += 1
                    BN2_accuracy[predict_field].append(1)
                    user_accuracy[user].append(1)
    
    BN2_perc = {key:round(np.mean(BN2_accuracy[key])*100,2) for key in BN2_accuracy.keys()}
    user_perc = {key:round(np.mean(user_accuracy[key])*100,2) for key in user_accuracy.keys()}
    print "\nTF App accuracy: ", TF_app_accuracy
    print "\nTF Call accuracy: ", TF_call_accuracy
    print "\nTF Cell accuracy: ", TF_cell_accuracy
    print "\nTF SMS accuracy: ", TF_sms_accuracy


    print "\nBN2 accuracy: "
    for each in BN2_perc:
        print each, ":", BN2_perc[each]
    
    print "\nUser accuracy: "
    for each in user_perc:
        print each, ":", user_perc[each]
                    
                    
def binary_classifier_dow(user, num_hours, start_date, cv_start, test_start):
    # binary classifier
    
    pipe = [{"$match": {"user":user}}]
    
    pos_apps = [0]*7
    neg_apps = [0]*7
    apps = db.command('aggregate', 'apps', pipeline=pipe)['result']
    train_apps = [each['date'] for each in apps if each['date'] < cv_start]
    
    pos_calls = [0]*7
    neg_calls = [0]*7
    calls = db.command('aggregate', 'calls', pipeline=pipe)['result']
    train_calls = [each['date'] for each in calls if each['date'] < cv_start]

    pos_cell = [0]*7
    neg_cell = [0]*7
    cell_data = db.command('aggregate', 'cell_data', pipeline=pipe)['result']
    train_cell = [each['date'] for each in cell_data if each['date'] < cv_start]

    pos_sms = [0]*7
    neg_sms = [0]*7
    sms_data = db.command('aggregate', 'sms', pipeline=pipe)['result']
    train_sms = [each['date'] for each in sms_data if each['date'] < cv_start]
        
    for day in xrange(int(num_hours*0.6/24)):
        dt = (start_date + timedelta(days=day)).weekday()

        train_apps_dow = [app for app in train_apps if app.weekday() is dt]
        if train_apps_dow:
            pos_apps[dt] += 1
        else:
            neg_apps[dt] += 1

        train_calls_dow = [call for call in train_calls if call.weekday() is dt]
        if train_calls_dow:
            pos_calls[dt] += 1
        else:
            neg_calls[dt] += 1
            
        train_cell_dow = [cell for cell in train_cell if cell.weekday() is dt]
        if train_cell_dow:
            pos_cell[dt] += 1
        else:
            neg_cell[dt] += 1

        train_sms_dow = [text for text in train_sms if text.weekday() is dt]
        if train_cell_dow:
            pos_sms[dt] += 1
        else:
            neg_sms[dt] += 1        
        
    pos_likelihood_apps = [(x+1) / (x + y+1) for x,y in zip(pos_apps, neg_apps)]
    neg_likelihood_apps = [(y+1) / (x + y+1) for x,y in zip(pos_apps, neg_apps)]
        
    pos_likelihood_calls = [(x+1) /(x + y+1) for x,y in zip(pos_calls, neg_calls)]
    neg_likelihood_calls = [(y+1) /(x + y+1) for x,y in zip(pos_calls, neg_calls)]

    pos_likelihood_cell = [(x+1) /(x + y+1) for x,y in zip(pos_cell, neg_cell)]
    neg_likelihood_cell = [(y+1) /(x + y+1) for x,y in zip(pos_cell, neg_cell)]

    pos_likelihood_sms = [(x+1) /(x + y+1) for x,y in zip(pos_sms, neg_sms)]
    neg_likelihood_sms = [(y+1) /(x + y+1) for x,y in zip(pos_sms, neg_sms)]
            
    return (pos_likelihood_apps, neg_likelihood_apps, pos_likelihood_calls,
            neg_likelihood_calls, pos_likelihood_cell, neg_likelihood_cell,
            pos_likelihood_sms, neg_likelihood_sms)
             
                    
def binary_classifier_hour(user, num_hours, start_date, cv_start, test_start):
    # binary classifier
    
    pipe = [{"$match": {"user":user}}]
    
    pos_apps = [0]*24
    neg_apps = [0]*24
    apps = db.command('aggregate', 'apps', pipeline=pipe)['result']
    train_apps = [each['date'] for each in apps if each['date'] < cv_start]
    cv_apps = [each['date'] for each in apps if each['date'] > cv_start and each['date'] < test_start]
    
    pos_calls = [0]*24
    neg_calls = [0]*24
    calls = db.command('aggregate', 'calls', pipeline=pipe)['result']
    train_calls = [each['date'] for each in calls if each['date'] < cv_start]
    cv_calls = [each['date'] for each in calls if each['date'] > cv_start and each['date'] < test_start]

    pos_cell = [0]*24
    neg_cell = [0]*24
    cell_data = db.command('aggregate', 'cell_data', pipeline=pipe)['result']
    train_cell = [each['date'] for each in cell_data if each['date'] < cv_start]
    cv_cell = [each['date'] for each in cell_data if each['date'] > cv_start and each['date'] < test_start]

    pos_sms = [0]*24
    neg_sms = [0]*24
    sms_data = db.command('aggregate', 'sms', pipeline=pipe)['result']
    train_sms = [each['date'] for each in sms_data if each['date'] < cv_start]
    cv_sms = [each['date'] for each in sms_data if each['date'] > cv_start and each['date'] < test_start]
        
    for hour in xrange(int(num_hours*0.6)):
        dt = start_date + timedelta(hours=hour)
        next_dt = dt + timedelta(hours = 1)

        train_apps_hour = [app for app in train_apps if app > dt and app < next_dt]
        if train_apps_hour:
            pos_apps[dt.hour] += 1
        else:
            neg_apps[dt.hour] += 1

        train_calls_hour = [call for call in train_calls if call > dt and call < next_dt]
        if train_calls_hour:
            pos_calls[dt.hour] += 1
        else:
            neg_calls[dt.hour] += 1
            
        train_cell_hour = [cell for cell in train_cell if cell > dt and cell < next_dt]
        if train_cell_hour:
            pos_cell[dt.hour] += 1
        else:
            neg_cell[dt.hour] += 1

        train_sms_hour = [text for text in train_sms if text > dt and text < next_dt]
        if train_cell_hour:
            pos_sms[dt.hour] += 1
        else:
            neg_sms[dt.hour] += 1        
        
    pos_prior_apps = sum(pos_apps)/(sum(pos_apps)+sum(neg_apps))
    neg_prior_apps = 1-pos_prior_apps
    pos_likelihood_apps = [x /(x + y) for x,y in zip(pos_apps, neg_apps)]
    neg_likelihood_apps = [y /(x + y) for x,y in zip(pos_apps, neg_apps)]
        
    pos_prior_calls = sum(pos_calls)/(sum(pos_calls)+sum(neg_calls))
    neg_prior_calls = 1-pos_prior_calls
    pos_likelihood_calls = [x /(x + y) for x,y in zip(pos_calls, neg_calls)]
    neg_likelihood_calls = [y /(x + y) for x,y in zip(pos_calls, neg_calls)]

    pos_prior_cell = sum(pos_cell)/(sum(pos_cell)+sum(neg_cell))
    neg_prior_cell = 1-pos_prior_cell
    pos_likelihood_cell = [x /(x + y) for x,y in zip(pos_cell, neg_cell)]
    neg_likelihood_cell = [y /(x + y) for x,y in zip(pos_cell, neg_cell)]
    
    pos_prior_sms = sum(pos_sms)/(sum(pos_sms)+sum(neg_sms))
    neg_prior_sms = 1-pos_prior_sms
    pos_likelihood_sms = [x /(x + y) for x,y in zip(pos_sms, neg_sms)]
    neg_likelihood_sms = [y /(x + y) for x,y in zip(pos_sms, neg_sms)]
            
    return (cv_apps, pos_prior_apps, neg_prior_apps, pos_likelihood_apps,
            neg_likelihood_apps, cv_calls, pos_prior_calls, neg_prior_calls,
            pos_likelihood_calls, neg_likelihood_calls, cv_cell, pos_prior_cell,
            neg_prior_cell, pos_likelihood_cell, neg_likelihood_cell, cv_sms,
            pos_prior_sms, neg_prior_sms, pos_likelihood_sms,
             neg_likelihood_sms)
             
def calc_place_likelihoods_dow(user_num, cv_start):
    pipe = [{"$match": {"user":user_num, 'place':"Home"}}]
    when_home = db.command('aggregate', 'places', pipeline=pipe)['result']
    train_home = [each['date'].weekday() for each in when_home if each['date'] < cv_start]
    total_hours = len(train_home)
    train_home_counter = Counter(train_home)
    home_likelihood = [0]*7
    for key in train_home_counter.keys():
        home_likelihood[key] = train_home_counter[key]/total_hours
        
     
    pipe = [{"$match": {"user":user_num, 'place':"Work"}}]
    when_work = db.command('aggregate', 'places', pipeline=pipe)['result']
    train_work = [each['date'].weekday() for each in when_work if each['date'] < cv_start]
    total_hours = len(train_work)   
    train_work_counter = Counter(train_work)
    work_likelihood = [0]*7
    for key in train_work_counter.keys():
        work_likelihood[key] = train_work_counter[key]/total_hours
    
    
    pipe = [{"$match": {"user":user_num, 'place':"Elsewhere"}}]
    when_else = db.command('aggregate', 'places', pipeline=pipe)['result']
    train_else = [each['date'].weekday() for each in when_else if each['date'] < cv_start]
    total_hours = len(train_else)
    train_else_counter = Counter(train_else)
    else_likelihood = [0]*7
    for key in train_else_counter.keys():
        else_likelihood[key] = train_else_counter[key]/total_hours
    
    pipe = [{"$match": {"user":user_num, 'place':"No signal"}}]
    when_no_sig = db.command('aggregate', 'places', pipeline=pipe)['result']
    train_no_sig = [each['date'].weekday() for each in when_no_sig if each['date'] < cv_start]
    total_hours = len(train_no_sig)
    train_no_sig_counter = Counter(train_no_sig)
    no_sig_likelihood = [0]*7
    for key in train_no_sig_counter.keys():
        no_sig_likelihood[key] = train_no_sig_counter[key]/total_hours  
      
    return home_likelihood, work_likelihood, else_likelihood, no_sig_likelihood 

                            
def calc_place_likelihoods_hour(user_num, cv_start):
    pipe = [{"$match": {"user":user_num, 'place':"Home"}}]
    when_home = db.command('aggregate', 'places', pipeline=pipe)['result']
    train_home = [each['date'].hour for each in when_home if each['date'] < cv_start]
    total_hours = len(train_home)
    train_home_counter = Counter(train_home)
    home_likelihood = [0]*24
    for key in train_home_counter.keys():
        home_likelihood[key] = train_home_counter[key]/total_hours
        
     
    pipe = [{"$match": {"user":user_num, 'place':"Work"}}]
    when_work = db.command('aggregate', 'places', pipeline=pipe)['result']
    train_work = [each['date'].hour for each in when_work if each['date'] < cv_start]
    total_hours = len(train_work)
    train_work_counter = Counter(train_work)
    work_likelihood = [0]*24
    for key in train_work_counter.keys():
        work_likelihood[key] = train_work_counter[key]/total_hours
    
    
    pipe = [{"$match": {"user":user_num, 'place':"Elsewhere"}}]
    when_else = db.command('aggregate', 'places', pipeline=pipe)['result']
    train_else = [each['date'].hour for each in when_else if each['date'] < cv_start]
    total_hours = len(train_else)
    train_else_counter = Counter(train_else)
    else_likelihood = [0]*24
    for key in train_else_counter.keys():
        else_likelihood[key] = train_else_counter[key]/total_hours    
    
    pipe = [{"$match": {"user":user_num, 'place':"No signal"}}]
    when_no_sig = db.command('aggregate', 'places', pipeline=pipe)['result']
    train_no_sig = [each['date'].hour for each in when_no_sig if each['date'] < cv_start]
    total_hours = len(train_no_sig)
    train_no_sig_counter = Counter(train_no_sig)
    no_sig_likelihood = [0]*24
    for key in train_no_sig_counter.keys():
        no_sig_likelihood[key] = train_no_sig_counter[key]/total_hours    
      
    return home_likelihood, work_likelihood, else_likelihood, no_sig_likelihood 
    
if __name__ == "__main__":
    BN_CV()
    
                    
                    
                    
                    
                    
                    
                    
