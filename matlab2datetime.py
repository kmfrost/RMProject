#!/usr/bin/python
import datetime
import sys


def matlab2datetime(matlab_datenum, shift=True):
    matlab_datenum = float(matlab_datenum)
    day = datetime.datetime.fromordinal(int(matlab_datenum))
    dayfrac = datetime.timedelta(days=matlab_datenum%1) - datetime.timedelta(days = 366)
    if shift:
        return day + dayfrac - datetime.timedelta(hours=4)
    else:
        return day + dayfrac
