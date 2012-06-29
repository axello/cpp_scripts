#!/usr/bin/python
#
# mlab_maxmind_processed.py
#
# Initials:
#               RB  Ruben Bloemgarten
#               AX  Axel Roest
#
# Version history
# 20120629      AX  first version, doesn't work yet
#
# test: 
# cd /DATA
# python scripts/mlab/mlab_mysql_import2.py mlab/clean/glasnost/20090128T000000Z-batch-batch-glasnost-0002.tgz.csv
# 
# ToDO: v loop over all arguments in sys.argv[0]
#       v deduplication toevoegen (put in hash, test on hash, clear hash for each file, but keep last entry
#       v move files naar archive directory
#       v move error files naar error directory
#       v log process and errors

import sys
import re
import os
from optparse import OptionParser
from datetime import datetime
import dateutil.parser as dparser
from dateutil.relativedelta import relativedelta
import MySQLdb

#################################################################
#                                                               #
#           settings                                            #
#                                                               #
#################################################################

# PLEASE UPDATE THESE SETTINGS
db_host = "localhost" # your host, usually localhost
db_user = "root" # your username
db_passwd = "rootpassword" # your password
db_name = "mlab" # name of the database
db_tables = {"glasnost": "glasnost", "ndt": "ndt"} # a mapping from testname to tablename
db_filetable = 'files'

# directories
baseDir     = '/DATA/mlab/'
scratchDir  = baseDir + 'scratch/'
workDir     = baseDir + 'work/'
archiveDir  = baseDir + 'archive/'
errorDir    = baseDir + 'error/'
logDir      = baseDir + 'logs/'
cleanDir    = baseDir + 'clean/'

#files
errorLog    = "error.log"
processLog  = "mlab_maxmind_processed.log"

#################################################################
#                                                               #
#           functions                                           #
#                                                               #
#################################################################

def usage():
  print "Usage: mlab_maxmind_processed.py maxmind_table"
  sys.exit(1)

# Blocks_GeoLiteCity_20090601
def extract_datestring(string):
  ''' Returns the datetime contained in string '''
  # Extract the date
  date_match = re.match('Blocks_GeoLiteCity_(\d{4}\d{2}\d{2})$', string)
  if not date_match:
    raise Exception('Error in argument "', string, '" does not contain a valid date.')
  return date_match.group(1)

def extract_date(string):
  ''' Returns the datetime contained in string '''
  # Extract the date
  date_match = re.match('.*(\d{4})(\d{2})(\d{2})$', string)
  if not date_match:
    raise Exception('Error in argument "', string, '" does not contain a valid date.')
  date = datetime(int(date_match.group(1)),int(date_match.group(2)),int(date_match.group(3)))
  return date
  
def exists_dbentry(cur, file_id, db_table, test_datetime, destination, source_ip):
    ''' Test if the entry already exists in the database '''
    # Check if the entry exists already 
    sql = "SELECT COUNT(*) FROM " + db_table + " WHERE date = '" + test_datetime.isoformat() + "' AND destination = '" + destination +  "' AND  source = '" + source_ip + "' AND file_id = " + str(file_id) 
    cur.execute(sql)

    if cur.fetchone()[0] < 1:
        return False
    else:
        return True

def blunt_insert_dbentry(cur, file_id, db_table, test_datetime, destination, source_ip):
    ''' Insert a connection to the database without testing '''
    columns = ', '.join(['date', 'destination', 'source', 'file_id'])
    values = '"' + '", "'.join([test_datetime.isoformat(), destination, source_ip, str(file_id)]) + '"'
    sql = "INSERT INTO  " + db_table + " (" + columns + ") VALUES(" + values + ") "
    cur.execute(sql)

def insert_dbentry(cur, file_id, db_table, test_datetime, destination, source_ip):
    ''' Insert a test connection to the database, if it not already exists '''
    # Check if the entry exists already 
    sql = "SELECT COUNT(*) FROM " + db_table + " WHERE date = '" + test_datetime.isoformat() + "' AND destination = '" + destination +  "' AND  source = '" + source_ip + "' AND file_id = " + str(file_id) 
    cur.execute(sql)

    # If not, then isert it
    if cur.fetchone()[0] < 1:
        print 'Found new test performed on the', test_datetime, 'from ' + destination + ' -> ' + source_ip + '.' 
        blunt_insert_dbentry(cur, file_id, db_table, test_datetime, destination, source_ip)

# return True if the table exists in the database
def check_maxmind_exist(cur, table):
    sql = "select * FROM maxmind.`" + table + "` LIMIT 1"
    cur.execute(sql)
    if cur.fetchone()[0] < 1:
        return False
    else:
        return True

def get_maxmind_dates(cur):
    datehash = {}
    sql = "SHOW TABLES FROM `maxmind`"
    cur.execute(sql)
    rows = cur.fetchall()
    rows2 = []
    # filter rows
    for item in rows:
        m = re.search('Blocks_GeoLiteCity_(\d+)$', item[0])
        if (m):
            rows2.append(m.group(0))

    # print rows2
    
    skipfirst = True
    for table in rows2:
        date = extract_datestring(table)
        if (skipfirst):
            skipfirst = False
        else:
            datehash[olddate] = date
        olddate = date
    # last one is 6 months in the future
    lastdate = extract_date(date)
    print 'date=' + date + " = " + str(lastdate)
    # futuredate = lastdate + datetime.timedelta(365 * 6/12)     
    futuredate = lastdate + relativedelta(months = +6)
    datehash[olddate] = futuredate.strftime('%Y%m%d')
    return datehash
    
def update_mlab_glasnost(cur,table):
    start_datum = extract_datestring(table)
    end_datum   = maxmind_dates[start_datum]
    print 'updating between' + start_datum + ' AND ' + end_datum
    try:
        sql = 'UPDATE mlab.glasnost SET locID = M.`locId` FROM mlab.glasnost L , maxmind.' + table + ' M WHERE  L.`source` BETWEEN M.`startnumip` AND M.`endnumip` AND L.`date` BETWEEN "' + start_datum + '" AND "' + end_datum + '" AND L.`locId` = 0'
        print sql
        cur.execute(sql)
    except MySQLdb.Error, e:
        print "An error has been passed. %s" %e
        
#################################################################
#                                                               #
#           start of initialisation                             #
#           Read command line options                           #
#                                                               #
#################################################################

parser = OptionParser()
parser.add_option("-q", "--quiet", action="store_false", dest="verbose", default=False, help="don't print status messages to stdout")
(options, args) = parser.parse_args()
if len(args) == 0:
  usage()

# create file if necessary, as open by itself doesn't cut it
f = open(logDir + processLog, 'a')
f.write("\nNew mlab_maxmind_processed job on " + str(datetime.now()))
f.close

#################################################################
#                                                               #
#           start of main program                               #
#                                                               #
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

# contains hash with key = start_date, value = enddate (= startdate of next table, except for the last one)
maxmind_dates = get_maxmind_dates(cur)
print maxmind_dates

# sys.exit(1)
# Iterate over ALL filenames
for table in args:
    if (check_maxmind_exist(cur,table)):
        update_mlab_glasnost(cur,table)

cur.close()
global_end_time = datetime.now()

print '=====================================\nAll Done. ' + str(len(args)) + ' file(s) in ' + str(global_end_time - global_start_time)
