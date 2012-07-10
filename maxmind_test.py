#!/usr/bin/python
#
# maxmind_test.py
#
# A test script for the maxmind class
#
# Initials:
#               AX  Axel Roest
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
import random
from maxmind import MaxMind

# test

mm = MaxMind(db_host, db_user, db_passwd, db_name, db_filetable)
ip = 67276848
loc = mm.lookup(ip)
print str(ip) + ' : ' + str(loc)

ip = 67277000
loc = mm.lookup(ip)
print str(ip) + ' : ' + str(loc)

ip = 67277023
loc = mm.lookup(ip)
print str(ip) + ' : ' + str(loc)

ip = 67277024
loc = mm.lookup(ip)
print str(ip) + ' : ' + str(loc)

testamount = 100000
start_time = datetime.now()
for i in range(testamount):
    r = random.randint(33996344, 3741319167)
    loc = mm.lookup(r)
end_time = datetime.now()
totaltime = end_time - start_time
timeperlookup = totaltime / testamount
print '=====================================\nLookup ' + str(testamount) + ' ips in ' +  str(totaltime) + ' seconds = ' + str(timeperlookup) + ' seconds per lookup'