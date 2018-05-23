'''
Created on May 17, 2018

@author: yaman
'''
# A simple distance-threashold calculator based on the average speed of 8 KM/hour.
# This is the space where a data science model could implement more complex logic.

from datetime import datetime


def distance_threshold(txn_time, loc_time):
    fmt = '%Y-%m-%d %H:%M:%S'
    t_time = datetime.strptime(txn_time, fmt)
    l_time = datetime.strptime(loc_time, fmt)
    time_diff = abs(l_time - t_time)
    s = time_diff.total_seconds()
    minute = 60
    if s > 10 * minute:
        return False
    else:
        # The average move is 8 kilometers in 10 minutes
        return 8000 * (s / 600)
