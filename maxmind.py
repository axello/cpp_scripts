#!/usr/bin/python
#
# maxmind.py
#
# A class to represent a full maxmind database as a python hash, to make locId lookups much faster (hopefully)
#
# Algorithm:
# set up two tables: 
#   startips = list of all the startips, we binary search through this using bisect
#   ipranges =  hash of all the ranges, where the key is the startip, and the endip is the value
# 
# binary search through startips to find the closest startip of the target ip
# get the endip of the range from the hash, and compare
# return locId to caller
#
# Initials:
#               AX  Axel Roest
#               RB  Ruben Bloemgarten
#
# Version history
# 20120710      AR  first version
#
# ToDO: 
#   

import sys
import re
import os
import math
import bisect
from datetime import datetime
import MySQLdb

#################################################################
#																#
#			settings											#
#																#
#################################################################

# Defaults

#################################################################
#																#
#			the meat											#
#																#
#################################################################

class MaxMind:
    # two tables:
    # startips = []   # list of all the startips, we binary search through this using bisect
    # ipranges = {}   # hash of all the ranges, where the key is the startip, and the endip is the value
    
    # initialise the object with the database table to instantiate with
    def __init__(self, maxmind_db_host, maxmind_db_user, maxmind_db_passwd, maxmind_db_name, maxmind_table_name):
        global_start_time = datetime.now()
        try:
            # Connect to the mysql database
            maxmind_db = MySQLdb.connect(host = maxmind_db_host, 
                                                     user = maxmind_db_user, 
                                                     passwd = maxmind_db_passwd, 
                                                     db = maxmind_db_name) 
            maxmind_cursor = maxmind_db.cursor()
            MaxMind.loadTable(self, maxmind_cursor, maxmind_table_name)
        except Exception as e:
            print('Aborting maxmind due to error: ' + str(e))
            exit(1)
        finally:
            maxmind_cursor.close()
        global_end_time = datetime.now()
        print 'MaxMind: Read and indexed `' + maxmind_table_name + '` in ' +  str(global_end_time - global_start_time) + ' seconds.'
    
    def loadTable(self, maxmind_cursor, maxmind_table_name):
        # grab everything (whole table, 3,5 million items)
        sql = """SELECT startIpNum, endIpNum, locId FROM `{0}`""".format(maxmind_table_name)
        maxmind_cursor.execute(sql)
        result = maxmind_cursor.fetchall()
        if result:
        # fromkeys
            self.startips = [x[0] for x in result]
            self.startips.sort()
            self.ipranges = {int(start) : [int(e),int(l)] for start,e,l in result}
        
    def find_le(self, a, x):
        # Find rightmost value less than or equal to x
        i = bisect.bisect_right(a, x)
        if i:
            return a[i-1]
        raise ValueError
    
    def lookup(self, ipnumber):
        # print "looking up: " + str(ipnumber)
        i = self.find_le(self.startips, ipnumber)
        # print "i="+str(i)
        (endip, loc) = self.ipranges[i]
        # print endip, loc
        if ipnumber <= endip:
            return loc
        else:
            return -1
        
