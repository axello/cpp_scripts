#!/usr/bin/python
#
# mlab_glasnost_update_all_locid.py
#
# Initials:
#				AX	Axel Roest
#
# Version history
# 20120706		AX	first version
#
# test: 


import sys
import re
import os
from optparse import OptionParser
from datetime import datetime
import dateutil.parser as dparser
from dateutil.relativedelta import relativedelta
from subprocess import call
import MySQLdb

#################################################################
#																#
#			settings											#
#																#
#################################################################

# PLEASE UPDATE THESE SETTINGS
db_host = "localhost" # your host, usually localhost
db_user = "root" # your username
db_passwd = "rootpassword" # your password
db_name = "mlab" # name of the database
db_tables = {"glasnost": "glasnost", "ndt": "ndt"} # a mapping from testname to tablename
db_filetable = 'files'

# directories
baseDir		= '/DATA/mlab/'
logDir		= baseDir + 'logs/'

#files
updatescript= "mlab_glasnost_update_locid.py"
errorLog	= "error.log"
processLog	= "mlab_maxmind_processed.log"


#################################################################
#																#
#			functions											#
#																#
#################################################################

# Blocks_GeoLiteCity_20090601
def extract_datestring(string):
	''' Returns the datetime contained in string '''
	# Extract the date
	date_match = re.match('Blocks_GeoLiteCity_(\d{4}\d{2}\d{2})$', string)
	if not date_match:
		raise Exception('Error in argument "', string, '" does not contain a valid date.')
	fulldate = date_match.group(1) + "000000"
	return fulldate

def extract_date(string):
	''' Returns the datetime contained in string '''
	# Extract the date
	date_match = re.match('.*(\d{4})(\d{2})(\d{2})000000$', string)
	if not date_match:
		raise Exception('Error in argument "', string, '" does not contain a valid date.')
	date = datetime(int(date_match.group(1)),int(date_match.group(2)),int(date_match.group(3)))
	return date

# return True if the table exists in the database
def check_maxmind_exist(cur, table):
	sql = "select * FROM maxmind.`" + table + "` LIMIT 1"
	cur.execute(sql)
	if cur.fetchone()[0] < 1:
		return False
	else:
		return True

# return a list of Blocks_GeoLiteCity_ tables, for looking up the locIds
def get_maxmind_tableset(cur):
	sql = "SHOW TABLES FROM `maxmind`"
	cur.execute(sql)
	allrows = cur.fetchall()
	rows = []
	# filter rows
	for item in allrows:
		m = re.search('Blocks_GeoLiteCity_(\d+)$', item[0])
		if (m):
			rows.append(m.group(0))
	return rows

def get_maxmind_dates(rows):
	datehash = {}
	# we skip storing the first entry, as we need the date of the second entry first to store the range
	skipfirst = True
	for table in rows:
		date = extract_datestring(table)
		if (skipfirst):
			skipfirst = False
		else:
			datehash[olddate] = date
		olddate = date
	# the skipping is set straight by storing the last entry outside of the loop
	# last one is 6 months in the future
	lastdate = extract_date(date)
	futuredate = lastdate + relativedelta(months = +6)
	datehash[olddate] = futuredate.strftime('%Y%m%d') + '000000'
	return datehash

def update_mlab_glasnost(cur,table):
	start_datum = extract_datestring(table)
	end_datum = maxmind_dates[start_datum]
	print 'updating `' + table + '` between ' + start_datum + ' - ' + end_datum
	try:
		# print ["/usr/bin/python", updatescript, table, "glasnost", start_datum, end_datum ]
		call(["/usr/bin/python", updatescript, table, db_tables['glasnost'], start_datum, end_datum ])
	except Exception as e:
		print "An error has occured: " +str(e)
		

#################################################################
#																#
#			start of initialisation								#
#			Read command line options							#
#																#
#################################################################

parser = OptionParser()
parser.add_option("-q", "--quiet", action="store_false", dest="verbose", default=False, help="don't print status messages to stdout")
(options, args) = parser.parse_args()
# check for -h argument here

# create file if necessary, as open by itself doesn't cut it
f = open(logDir + processLog, 'a')
f.write("\nNew mlab_glasnost_update_all job on " + str(datetime.now()))
f.close


#################################################################
#																#
#			start of main program								#
#																#
#################################################################
global_start_time = datetime.now()

try:
	# Connect to the mysql database
	db = MySQLdb.connect(host = db_host, 
						 user = db_user, 
						 passwd = db_passwd, 
						 db = db_name) 
	cur = db.cursor()
	
except:
	sys.stderr.write('Error, cannot connect to database' + db_name + '\n')

# array with tables to loop over, in case we don't get a table argument
maxmind_all_tables = get_maxmind_tableset(cur)

# contains hash with key = start_date, value = enddate (= startdate of next table, except for the last one)
maxmind_dates = get_maxmind_dates(maxmind_all_tables)

if len(args) == 0:
	print "Iterating over ALL maxmind tables"
	for table in maxmind_all_tables:
		if (check_maxmind_exist(cur,table)):
			update_mlab_glasnost(cur,table)
else:
	print "Iterating over all arguments"
	for table in args:
		if (check_maxmind_exist(cur,table)):
			update_mlab_glasnost(cur,table)

cur.close()
global_end_time = datetime.now()

print '=====================================\nAll Glasnost updates Done. ' + str(len(args)) + ' file(s) in ' + str(global_end_time - global_start_time)
