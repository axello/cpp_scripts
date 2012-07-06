#!/usr/bin/python
#
# mlab_glasnost_update_locid.py
#
# Initials:
#				PH	Pascal Haakmat
#				AR	Axel Roest
#
# Version history
# 20120706		PH	first version based on mlab_maxmind_processed.py
# 20120707		AR	tested on glasnost_test database, update query changed, added location fields update
#
# Note: make sure the glasnost table has separate indexes on longip 
# and date columns, i.e.:
#	KEY `longip` (`longip`),
#	KEY `date` (`date`) 
# as part of the schema definition. Do not index locId - this will
# slow things down because MySQL needs to maintain the index for the field 
# as we update it.
#
# Example invocation:
# $ python mlab_glasnost_update_locid.py Blocks_GeoLiteCity glasnost 19000101 30000101
# 2012-07-06 04:28:08  Starting geoIP lookup for date range 19000101-30000101
# 2012-07-06 04:28:08  Total maxmind rows: 3786204
# 2012-07-06 04:28:08  Processing in chunks of 100000 records (38 chunk(s))
# 2012-07-06 04:28:08  Processing chunk #1 (0-99999)
# ...
# 2012-07-06 04:39:58  Processing chunk #38 (3700000-3786203)
# 2012-07-06 04:40:16  Total glasnost changes: 935005
# 2012-07-06 04:40:16  Finished in 0:12:07.507977

import sys
import re
import os
import math
from datetime import datetime
import dateutil.parser as dparser
from dateutil.relativedelta import relativedelta
import MySQLdb

#################################################################
#																#
#			settings											#
#																#
#################################################################

# Defaults
maxmind_db_host = "localhost" # your host, usually localhost
maxmind_db_user = "dbuser" # your username
maxmind_db_passwd = "password" # your password
maxmind_db_name = "chokepoint_mlab" # name of the database
maxmind_table_name = 'Blocks_GeoLiteCity'

glasnost_db_host = maxmind_db_host
glasnost_db_user = maxmind_db_user
glasnost_db_passwd = maxmind_db_passwd
glasnost_db_name = "mlab"
glasnost_table_name = 'glasnost_test'


#################################################################
#																#
#			the meat											#
#																#
#################################################################

def log(str):
	print datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "	" + str
	sys.stdout.flush()

def update_glasnost(glasnost_cursor,glasnost_table_name,start_date,end_date,start_ip,end_ip,loc_id,maxmind_table_name):
	updated = 0
	try:
#		sql = """UPDATE `{0}` SET `locId`={1} WHERE `locId`=0 AND `longip` BETWEEN {2} AND {3} AND `date` BETWEEN {4} AND {5}""".format(glasnost_table_name, loc_id, start_ip, end_ip, start_date, end_date)
		sql = """UPDATE `{0}` SET `locId`={1} , `maxmind_table_name` = '{2}' WHERE `longip` BETWEEN {3} AND {4} AND `date` BETWEEN {5} AND {6}""".format(glasnost_table_name, loc_id, maxmind_table_name, start_ip, end_ip, start_date, end_date)
		glasnost_cursor.execute(sql)
		updated = glasnost_cursor.rowcount
	except MySQLdb.Error, e:
		log("Error updating glasnost: {0}".format(e))
	return updated

def update_glasnost_with_location(glasnost_cursor,glasnost_table_name,location_table_name):
	updated = 0
	print glasnost_table_name + " - with - " + maxmind_table_name
	try:
		sql = """UPDATE mlab.`{0}` L, maxmind.`{1}` M SET L.country_code=M.country, L.region=M.region, L.city=M.city, L.postalCode=M.postalCode, L.latitude=M.latitude, L.longitude=M.longitude, L.metroCode=M.metroCode, L.areaCode=M.areaCode WHERE L.`locId` = M.`locId`""".format(glasnost_table_name, location_table_name)
		glasnost_cursor.execute(sql)
		updated = glasnost_cursor.rowcount
	except MySQLdb.Error, e:
		log("Error updating glasnost: {0}".format(e))
	return updated

def process(maxmind_cursor,maxmind_table_name,glasnost_cursor,glasnost_table_name,glasnost_start_date,glasnost_end_date):
	location_table_name = maxmind_table_name.replace("Blocks", "Location")
	sql = """SELECT COUNT(*) FROM `{0}`""".format(maxmind_table_name)
	maxmind_cursor.execute(sql)
	result = maxmind_cursor.fetchone()

	maxmind_total = result[0]

	log('Total maxmind rows: '+ str(maxmind_total))

	chunk_size = 100000
	chunk_count = int(math.ceil(float(maxmind_total) / float(chunk_size)))

	log('Processing in chunks of '+ str(chunk_size) + ' records (' + str(chunk_count) + ' chunk(s))')

	glasnost_changes = 0
	chunk_num = 1

	for offset in range(0, maxmind_total, chunk_size):
		count = min(chunk_size, maxmind_total - offset)

		log('Processing chunk #' + str(chunk_num) + ' (' + str(offset) + '-' + str(offset - 1 + count) + ')')

		sql = """SELECT * FROM `{0}` LIMIT {1} OFFSET {2}""".format(maxmind_table_name, count, offset)
		maxmind_cursor.execute(sql)
		result = maxmind_cursor.fetchall()

		for row in result:
			glasnost_changes = glasnost_changes + update_glasnost(glasnost_cursor,glasnost_table_name,glasnost_start_date,glasnost_end_date,row[0],row[1],row[2], location_table_name)	  

		chunk_num = chunk_num + 1
		
	# update the rest of the fields from the maxmind location table
	fieldupdates = update_glasnost_with_location(glasnost_cursor,glasnost_table_name,location_table_name)
	log('Total glasnost changes: ' + str(glasnost_changes) + ' [location updates: ' + str(fieldupdates) + ']')

	return glasnost_changes

#################################################################
#																#
#			start of initialisation								#
#			Read command line options							#
#																#
#################################################################

if(len(sys.argv) != 5):
	print 'usage: {0} maxmind_table_name glasnost_table_name start_date end_date'.format(os.path.basename(sys.argv[0]))
	print 'start_date and end_date should be of the format YYYYMMDDHHMMSS'
	exit(1)

maxmind_table_name = sys.argv[1]
glasnost_table_name = sys.argv[2]
start_date = sys.argv[3]
end_date = sys.argv[4]

#################################################################
#																#
#			start of main program								#
#																#
#################################################################
global_start_time = datetime.now()

log('Starting geoIP lookup for date range {0}-{1}'.format(start_date, end_date))

try:
	# Connect to the mysql database
	maxmind_db = MySQLdb.connect(host = maxmind_db_host, 
											 user = maxmind_db_user, 
											 passwd = maxmind_db_passwd, 
											 db = maxmind_db_name) 
	maxmind_cursor = maxmind_db.cursor()
	glasnost_db = MySQLdb.connect(host = glasnost_db_host, 
											user = glasnost_db_user, 
											 passwd = glasnost_db_passwd, 
											 db = glasnost_db_name) 
	glasnost_cursor = glasnost_db.cursor()
	process(maxmind_cursor,maxmind_table_name,glasnost_cursor,glasnost_table_name,start_date,end_date)
except Exception as e:
	log('Aborting due to error: ' + str(e))
	exit(1)
finally:
	maxmind_cursor.close()
	glasnost_cursor.close()

global_end_time = datetime.now()

log('Finished in ' + str(global_end_time - global_start_time))
