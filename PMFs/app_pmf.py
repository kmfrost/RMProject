from __future__ import division
import sys
sys.path.insert(0, '../')
import scipy.io
import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
from matlab2datetime import matlab2datetime
import numpy as np
import matplotlib.pyplot as plt
import sys
from bson.son import SON
from collections import defaultdict, Counter

def app_hist(user_num, metric, group_time=0):
    #setup the database
    try:
        client = pymongo.MongoClient()
        print "\nConnected to MongoDB\n"
    except pymongo.errors.ConnectionFailure, e:
        print "Could not connect to MongoDB: %s" % e
    db = client.rm_db
    apps = db.apps
    places = db.places

    metric_dict={1: "$app_date", 2: "$app_name", 3:"$app_end", 4:"$place"}

    if metric is 1:
        pipe = [{"$match": {"user":user_num}}]
        result = db.command('aggregate', 'apps', pipeline=pipe)

        times = []
        for each_bin in result['result']:
            times.append(each_bin['app_date'])

        if not times:
            pass
        elif group_time is 0:  # group by hour
            width = 1
            hours = [t.hour for t in times]
            bins, values = zip(*Counter(hours).items())
            values = [val+1 for val in values]  # smooth
            total = sum(values)
            values = [(val)/total for val in values]
            plt.bar(bins, values, width) # to bin by hour
            plt.title('PMF of app usage times by hour for user #{num}'.format(num=user_num))
            plt.xlabel('Hour')
            plt.xticks([x + width*0.5 for x in xrange(24)], xrange(24))
            plt.show()
        elif group_time is 1:  # group by day of the week
            weekdays = [t.weekday() for t in times]
            bins, values = zip(*Counter(weekdays).items())
            width = 1
            values = [val+1 for val in values]  # smooth
            total = sum(values)
            values = [(val)/total for val in values]
            plt.bar(bins, values, width)
            plt.xlabel('Day of the week')
            plt.xticks([x + width*0.5 for x in bins], ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
            plt.title('PMF of app usage times by day of the week for user #{num}'.format(num=user_num))
            plt.show()
        elif group_time is 2:  # group by day of the month
            width = 1
            days = [t.day for t in times]
            bins, values = zip(*Counter(days).items())
            values = [val+1 for val in values]  # smooth
            total = sum(values)
            values = [(val)/total for val in values]
            plt.bar(bins, values, width)
            plt.xticks([x + width*0.5 for x in xrange(1, 32)], xrange(1, 32))
            plt.title('PMF of app usage times by day of the month for user #{num}'.format(num=user_num))
            plt.xlabel('Day of the month')
            plt.show()
        else:  # group by month
            months = [t.month for t in times]
            bins, values = zip(*Counter(months).items())
            values = [val+1 for val in values]  # smooth
            total = sum(values)
            values = [(val)/total for val in values]
            width = 1
            plt.bar(bins, values, width)
            plt.xticks([x+width*0.5 for x in xrange(1,13)], ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'])
            plt.title('PMF of tower transitions by month for user #{num}'.format(num=user_num))
            plt.xlabel('Month')
            plt.title('PMF of app usage times by month for user #{num}'.format(num=user_num))
            plt.show()

    elif metric is 3:  # app duration
        pipe = [{"$match": {"user":user_num}}]
        result = db.command('aggregate', 'apps', pipeline=pipe)
        bins = defaultdict(list)
        for each_bin in result['result']:
            bins[each_bin['app_name']].append(each_bin['app_duration'])
        averages = {}
        for each_bin in bins:
            avg_secs = sum(bins[each_bin])/len(bins[each_bin])
            averages[each_bin] = avg_secs/60.0  # change to average minutes
        if not averages:
            print "No app duration data for user #{num}\n".format(num=user_num)
        else:
            y = sorted(averages.values())
            x = sorted(averages, key=averages.get)
            values = [val+1 for val in y]  # smooth
            total = sum(values)
            y = [(val)/total for val in values]


            plt.bar(range(len(x)), y, align='center')
            plt.xticks(range(len(x)),x, size='small')
            plt.title('PMF of average app duration times for user #{num}\n'.format(num=user_num))
            plt.xlabel('App Name')
            plt.ylabel('Average time in use (minutes)')
            plt.show()

    elif metric is 2:
        if group_time is 0:
            pipe = [{"$match": {"user":user_num}}, {"$group": {"_id":metric_dict[metric], "count": {"$sum":1}}}, {"$sort": SON([("count", -1), ("_id", -1)])}]
            result = db.command('aggregate', 'apps', pipeline=pipe)
            bins = {}
            for each_bin in result['result']:
                bins[each_bin['count']] = each_bin['_id']
            x = bins.keys()
            x.sort()
            y = [bins[key] for key in x]
            values = [val+1 for val in x]  # smooth
            total = sum(values)
            x = [(val)/total for val in values]

            plt.bar(range(len(x)), x, align='center')
            plt.xticks(range(len(y)), y, size='small')
            plt.title("PMF of each app's access probability for user #{num}".format(num=user_num))
            plt.xlabel('App Name')
            plt.ylabel('Probability of access')
            plt.show()
            
        elif group_time is not -1:
            pipe = [{"$match": {"user":user_num}}]           
            result = db.command('aggregate', 'apps', pipeline=pipe)
            result = [each_app for each_app in result['result'] if each_app['app_date'].hour == group_time]
            app_list = [each_app['app_name'] for each_app in result]
            exclude_apps = [u'Phone', u'Phonebook', u'Menu', u'mce', u'ScreenSaver', u'context_log', u'Logs', u'BtUi', u'MGS', u'www', u'gs', u'JavaAware', u'profileapp', u'Appmngr', u'MobileSense', u'exbubble', u'AgileMessenger', u'Appinst', u'Cam', u'ConnectionMonitorUi', u'CtrlFreak', u'KPCaMain', u'MupeClient', u'NpdViewer', u'PSLN', u'Pang', u'Satui', u'Sos', u'Switcher', u'SysAp', u'Ussd', u'VCommand', u'contextbook', u'cshelp', u'mixpix', u'mmcapp', u'CbsUiApp', u'Emonic', u'FileManager', u'FaxModemUi', u'FExplorer']
    
            app_list = [each_app for each_app in app_list if each_app not in exclude_apps]
            app_counts = Counter(app_list)
            y = sorted(app_counts.values())
            x = sorted(app_counts, key=app_counts.get)

            values = [val+1 for val in y]  # smooth
            total = sum(values)
            y = [(val)/total for val in values]

            plt.bar(range(len(y)), y, align='center')
            plt.xticks(range(len(x)), x, size='small')
            plt.title("PMF of each app's access probability for user #{num}".format(num=user_num))
            plt.xlabel('App Name')
            plt.ylabel('Probability of access')
            plt.show()
        else:
            pipe = [{"$match": {"user":user_num}}, {"$group": {"_id":metric_dict[metric], "count": {"$sum":1}}}, {"$sort": SON([("count", -1), ("_id", -1)])}]
            result = db.command('aggregate', 'apps', pipeline=pipe)
            bins = {}
            for each_bin in result['result']:
                bins[each_bin['_id']] = each_bin['count']
            exclude_apps = [u'Phone', u'Phonebook', u'Menu', u'mce', u'ScreenSaver', u'context_log', u'Logs', u'BtUi', u'MGS', u'www', u'gs', u'JavaAware', u'profileapp', u'Appmngr', u'MobileSense', u'exbubble', u'AgileMessenger', u'Appinst', u'Cam', u'ConnectionMonitorUi', u'CtrlFreak', u'KPCaMain', u'MupeClient', u'NpdViewer', u'PSLN', u'Pang', u'Satui', u'Sos', u'Switcher', u'SysAp', u'Ussd', u'VCommand', u'contextbook', u'cshelp', u'mixpix', u'mmcapp', u'CbsUiApp', u'Emonic', u'FileManager', u'FaxModemUi', u'FExplorer']
                
            for each_app in exclude_apps:
                try:
                    del bins[each_app]
                except KeyError:
                    pass
                
            y = sorted(bins.values())
            x = sorted(bins, key=bins.get)
            values = [val+1 for val in y]  # smooth
            total = sum(values)
            y = [(val)/total for val in values]

            plt.bar(range(len(y)), y, align='center')
            plt.xticks(range(len(x)), x, size='small')
            plt.title("PMF of each app's access probability for user #{num}".format(num=user_num))
            plt.xlabel('App Name')
            plt.ylabel('Probability of access')
            plt.show()
    else:
        place_dict = {1: "Home", 2: "Work", 3:"Elsewhere", 4:"No signal"}
        pipe = [{"$match": {"user":user_num}}]           
        app_result = db.command('aggregate', 'apps', pipeline=pipe)['result']
        pipe2 = [{"$match": {"user": user_num, "place": place_dict[group_time]}}]
        place_result = db.command('aggregate', 'places', pipeline=pipe)['result']
        app_list = [each_app['app_name'] for each_app in result]
        exclude_apps = [u'Phone', u'Phonebook', u'Menu', u'mce', u'ScreenSaver', u'context_log', u'Logs', u'BtUi', u'MGS', u'www', u'gs', u'JavaAware', u'profileapp', u'Appmngr', u'MobileSense', u'exbubble', u'AgileMessenger', u'Appinst', u'Cam', u'ConnectionMonitorUi', u'CtrlFreak', u'KPCaMain', u'MupeClient', u'NpdViewer', u'PSLN', u'Pang', u'Satui', u'Sos', u'Switcher', u'SysAp', u'Ussd', u'VCommand', u'contextbook', u'cshelp', u'mixpix', u'mmcapp', u'CbsUiApp', u'Emonic', u'FileManager', u'FaxModemUi', u'FExplorer']
        
            


if __name__ == "__main__":
    app_hist(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]))
