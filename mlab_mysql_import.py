#!/usr/bin/python
#
# Initials:     SF  Simon Funke
#               RB  Ruben Bloemgarten
#               AX  Axel Roest
#
# Version history
# 2012xxxx      SF  first version
# 20120628      AX  removed testing for every line, added timing code, 
# 20120629      AX  added loop over all arguments, exception handling, restructured code, moved processed files to archive or error folder
# 20120708      AX  skip empty ip lines instead or error message
# 20120708      RB  cleaning some names and spelling, also we don't want processed_files.log to clobber the downloaders processed_files.log. So we should use overly descriptive names
# 20120710      AX  added locId lookup and added longip to insert query
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
#       v skip empty ip lines instead or error message
#       v added locId lookup and added longip to insert query
#
#       Get the date from the filename, and look up the correct maxmind database
#       then, insert the locId directly with the line in the mlab/{glasnost,ndt} database, preventing slow future updates
#       on the other hand, all these updates might be extremely slow: TEST
#
#       todo : refactor all the utility functions in a separate file
#       todo : refactor all the passwords in a separate file (which is NOT in the repo, AND is in the .gitignore list

import sys
import re
import os
from optparse import OptionParser
from datetime import datetime
import dateutil.parser as dparser
import MySQLdb
import shutil
from maxmind import MaxMind
import socket, struct

#################################################################
#                                                               #
#           settings                                            #
#                                                               #
#################################################################

# PLEASE UPDATE THESE SETTINGS
db_host = "localhost" # your host, usually localhost
db_user = "root" # your username
db_passwd = "" # your password
db_name = "mlab" # name of the database
db_tables = {"glasnost": "glasnost", "ndt": "ndt_test"} # a mapping from testname to tablename
db_filetable = 'files'

# directories
baseDir     = '/DATA/mlab/'
#baseDir     = '/home/axel/mlab/'
scratchDir  = baseDir + 'scratch/'
workDir     = baseDir + 'work/'
archiveDir  = baseDir + 'archive/'
errorDir    = baseDir + 'error/'
logDir      = baseDir + 'logs/'
cleanDir    = baseDir + 'clean/'

#files
errorLog    = "mlab_mysql_import_error.log"
processLog  = "mlab_mysql_import_processed_files.log"

# default tables
maxmind_table = 'Blocks_GeoLiteCity_Last'
ndt_import    = 'ndt_import'
#################################################################
#                                                               #
#           functions                                           #
#                                                               #
#################################################################

# Convert an IP string to long
def ip2long(ip):
  packedIP = socket.inet_aton(ip)
  return struct.unpack("!L", packedIP)[0]

def long2ip(l):
  return socket.inet_ntoa(struct.pack('!L', l))

def usage():
  print "Usage: mlab_mysql_import.py [ -m maxmind_Blocks_Tablename ] mlab_file1.csv [mlab_files.csv ...]"
  print "Default: maxmind_Blocks_Tablename = `Blocks_GeoLiteCity_Last`"
  sys.exit(1)

# This routine extracts the destination server of the mlab file. 
# It assumes that the filename has the form like 20100210T000000Z-mlab3-dfw01-ndt-0000.tgz.csv
#  
def extract_destination(filename):
  # Split the filename and perform some tests if it conforms to our standard
  f_split = filename.split('-')
  if len(f_split) < 3:
    raise Exception("The specified filename (", filename, ") should contain at least two '-' characters that delimit the data, destination and the suffix.")
    
  if '.tgz.csv' not in f_split[-1]:
    print "The specified filename (", filename, ") should end with '.tgz.csv'."

  return '.'.join(filename.split('-')[1:-1])

# Returns the datetime contained in string.
def extract_datetime(string):
  # Extract the date
  date_match = re.search(r'\d{4}/\d{2}/\d{2}', string)
  if not date_match:
    raise Exception('Error in import: line "', string, '" does not contain a valid date.')
  # Extract the time
  time_match = re.search(r'\d{2}:\d{2}:\d{2}', string)
  if not time_match:
    raise Exception('Error in import: line "', string, '" does not contain a valid time.')

  try:
    return dparser.parse(date_match.group(0) + ' ' + time_match.group(0), fuzzy=True) 
  except ValueError:
    raise ValueError, 'Error in import: line "' + string + '" does not contain a valid date and time.'

# Returns the first valid ip address contained in string.
# return with empty string when we encounter cputime, or no ip number
def extract_ip(string):
  if re.search('cputime', string):
    return ''
  # Extract the date
  match = re.search(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', string)
  if not match:
    # ignore file
    return ''
    # raise Exception ('Error in import: line "', string, '" does not contain a valid ip address.')
  return match.group(0)

# Test if the entry already exists in the database
def exists_dbentry(cur, file_id, db_table, test_datetime, destination, source_ip):
    # Check if the entry exists already 
    sql = "SELECT COUNT(*) FROM " + db_table + " WHERE date = '" + test_datetime.isoformat() + "' AND destination = '" + destination +  "' AND  source = '" + source_ip + "' AND file_id = " + str(file_id) 
    cur.execute(sql)

    if cur.fetchone()[0] < 1:
        return False
    else:
        return True

# Insert a connection to the database without testing.
def blunt_insert_dbentry(cur, file_id, db_table, test_datetime, destination, source_ip):
    longip = ip2long(source_ip)
    # locid = 0
    locid = mm.lookup(longip)                  # lookup location id from ip number
    columns = ', '.join(['date', 'destination', 'source', 'file_id', 'longip', 'locId'])
    values = '"' + '", "'.join([test_datetime.isoformat(), destination, source_ip, str(file_id), str(longip), str(locid)]) + '"'
    sql = "INSERT INTO  " + db_table + " (" + columns + ") VALUES(" + values + ") "
    # print sql
    cur.execute(sql)

# Insert a test connection to the database, if it not already exists
def insert_dbentry(cur, file_id, db_table, test_datetime, destination, source_ip):
    # Check if the entry exists already 
    sql = "SELECT COUNT(*) FROM " + db_table + " WHERE date = '" + test_datetime.isoformat() + "' AND destination = '" + destination +  "' AND  source = '" + source_ip + "' AND file_id = " + str(file_id) 
    cur.execute(sql)

    # If not, then insert it
    if cur.fetchone()[0] < 1:
        print 'Found new test performed on the', test_datetime, 'from ' + destination + ' -> ' + source_ip + '.' 
        blunt_insert_dbentry(cur, file_id, db_table, test_datetime, destination, source_ip)

# Returns the id of a filename in the filename table. Creates a new row if the filename does not exist. 
def get_file_id(cur, filename):
    sql = "SELECT id FROM " + db_filetable + " WHERE filename ='" + filename + "'"
    cur.execute(sql)
    id = cur.fetchone()
    # If the entry does not exist, we add it in
    if not id:
        sql = "INSERT INTO  " + db_filetable + " (filename) VALUES('" + filename + "')"
        cur.execute(sql)
        return get_file_id(cur, filename)
    return id[0]

# do deduplucation of connection strings
def dedup(file_id, table, test_datetime, destination, source_ip):
    key = str(file_id) + table + str(test_datetime) + destination + source_ip
    if key in deduplookup:
        return False
    else:
        deduplookup[key] = True
        return True
        
# for the temp table, look up all the locations with the locId
def lookup_locations(cur, destination):
    location_table_name = maxmind_table.replace("Blocks", "Location")
    # sql = 'UPDATE mlab.`' + destination + '` L, maxmind.`' + location_table_name + '` M SET L.country_code = M.country, L.region=M.region, L.city=M.city, L.postalCode=M.postalCode, L.latitude=M.latitude, L.longitude=M.longitude, L.metroCode=M.metroCode, L.areaCode=M.areaCode WHERE L.`locId` = M.`locId`'
    sql = 'UPDATE mlab.`ndt_import` L, maxmind.`' + location_table_name + '` M SET L.country_code = M.country, L.region=M.region, L.city=M.city, L.postalCode=M.postalCode, L.latitude=M.latitude, L.longitude=M.longitude, L.metroCode=M.metroCode, L.areaCode=M.areaCode WHERE L.`locId` = M.`locId`'
    updated = cur.execute(sql)
    # update country from country_code later?
    return updated

# clear the temp table
def clear_temp_table(cur):
    sql = 'truncate table `' + ndt_import + '`'
    cur.execute(sql)
    
# move the temp table to the real on (either ndt_test or ndt)
def move_temp_table(cur, destination):
    sql = 'INSERT INTO `' + destination + '` (`created_at`, `date`, `destination`, `source`, `file_id`, `country_code`, `longip`, `locId`, `country`, `region`, `city`, `postalCode`, `latitude`, `longitude`, `metroCode`, `areaCode`)  SELECT * FROM `' + ndt_import + '`'
    updated = cur.execute(sql)
    return updated
    
# returns True on error, False on correct processing
def process_file(f, filename):
    start_time = datetime.now()
    failure = True
    try:
        # Connect to the mysql database
        db = MySQLdb.connect(host = db_host, 
                             user = db_user, 
                             passwd = db_passwd, 
                             db = db_name) 
        cur = db.cursor() 
        clear_temp_table(cur)
    
        # Find the destination server by investigating the filename
        destination = extract_destination(filename)
        print 'Destination: ', destination,
    
        # Get the filename id from the files table
        file_id = get_file_id(cur, filename) 
        db.commit()
    
        # Find the testsuite (glasnost or ndt) by investigating the filename
        try:
            test = [test for test in db_tables.keys() if test in filename][0]
        except IndexError:
            sys.stderr.write('The filename ' + filename + ' does not contain a valid testname.')
            return 1
        # print "Found test suite " + test 
    
        # The filetest ALONE, takes 3 seconds with a 9 million records database, without indexes
        # But falls back to less than half a second when indexing is turned on on the db
        filetest=True
        # Read the file line by line and import it into the database
        for line in f:
          line = line.strip()
          source_ip = extract_ip(line)
          if ('' == source_ip):
            continue                    # skip empty lines instead of error reporting them
          test_datetime = extract_datetime(line)
          if (filetest):
            if (exists_dbentry(cur, file_id, db_tables[test], test_datetime, destination, source_ip)):
                # this file has already been read: ABORT WITH ERROR
                raise Exception('File entry already exist in db; the file has already been read: ' + filename)
            filetest=False
          # test if we have already done it in this or last filetest
          if (dedup(file_id, db_tables[test], test_datetime, destination, source_ip)):
              # blunt_insert_dbentry(cur, file_id, db_tables[test], test_datetime, destination, source_ip)
              blunt_insert_dbentry(cur, file_id, ndt_import, test_datetime, destination, source_ip)
        end_time = datetime.now()
        print 'File done in ' + str(end_time - start_time)
        lookup_locations(cur, destination)
        move_temp_table(cur, db_tables[test])
        failure = False
    except Exception as inst:
        sys.stderr.write('Exception: '+str(inst.args)  + '\n')
        with open(logDir + errorLog, 'a') as f:
            f.write(pathname + '\n')
            f.write('Exception: '+str(inst.args)  + '\n')
        print
    except IOError as e:
        sys.stderr.write('Error handling file ' + filename + ' (' + str(e.args) + ')\n')
        with open(logDir + errorLog, 'a') as f:
            f.write(pathname + '\n')
            f.write('Error handling file ' + filename + ' (' + str(e.args) + ')\n')
        print
    finally:
        # Commit and finish up
        sys.stderr.flush()
       # db.commit()
        # disconnect from server
        db.close()
    
    return failure
    
# get the test date from the archive filename
def extract_archive_date(filename):
      m = re.match('^(\d{4})(\d{2})(\d{2})', filename)
      return (m.group(1),m.group(2))

# test if archive directory exist, and create it if necessary
def create_archive_dir(ym):
    if (not os.path.exists(ym)):
        os.makedirs(ym)
    return ym

# move processed file to archive folder
def move_archive(pathname):
    fname = os.path.basename(pathname)
    (year,month) = extract_archive_date(fname)
    aDir = create_archive_dir(archiveDir + year +'/'+ month)
    shutil.move(pathname,aDir)
    with open(logDir + processLog, 'a') as f:
        f.write(pathname + '\n')


#################################################################
#                                                               #
#           start of initialisation                             #
#           Read command line options                           #
#                                                               #
#################################################################

parser = OptionParser()
parser.add_option("-q", "--quiet", action="store_false", dest="verbose", default=False, help="don't print status messages to stdout")
parser.add_option("-m", "--maxmind", dest="maxmind_table", default='', help="optional maxmind_table, if omitted we use 'Last'")
(options, args) = parser.parse_args()
if options.maxmind_table != '':
    maxmind_table = options.maxmind_table
    
if len(args) == 0:
  usage()

# create file if necessary, as open by itself doesn't cut it
f = open(logDir + processLog, 'a')
f.write("\nNew batchjob on " + str(datetime.now()))
f.close

# deduplookup is a hash we use for de-duplication of input lines
# maybe it is necessary to purge parts of it during the duration of the import
# but then we have to carefully monitor tests that appear in multiple files
# OR store the last test in a separate global (dirty? yeah, I know)
deduplookup = {}

#################################################################
#                                                               #
#           start of main program                               #
#                                                               #
#################################################################
global_start_time = datetime.now()

# get instance of maxmind table
print "using " + maxmind_table

mm = MaxMind(db_host, db_user, db_passwd, "maxmind",maxmind_table)

if not mm:
    sys.stderr.write('maxmind table does not exist: ' + maxmind_table + ' (' + str(e.args) + ')\n')
    exit(1)

# Iterate over ALL filenames
for pathname in args:
    try:
         with open(pathname, 'r') as f:
            # Extract the basename of the filename, as the path is not of interest after this point
            filename = os.path.basename(pathname)
            print "processing file " + filename,
            if (process_file(f, filename)):
                shutil.move(pathname,errorDir)
            else:
                move_archive(pathname)
        # file is automatically closed if needed
    except IOError as e:
         print 'Could not open file ' + pathname + '\nError: ' + str(e.args)

global_end_time = datetime.now()

print '=====================================\nAll Done. ' + str(len(args)) + ' file(s) in ' + str(global_end_time - global_start_time)
