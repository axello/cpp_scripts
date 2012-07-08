#!/usr/bin/python
# argument test

import sys
import re
import os
import shutil
from optparse import OptionParser

# Read command line options
def usage():
  print "Usage: mlab_mysql_import.py mlab_file.csv"
  print "Recursive import can be realised by running:"
  print "find . -iname '*.tgz.csv' -exec ./mlab_mysql_import.py {} \;"
  sys.exit(1)


#################################################################
#                                                               #
#           start of main program                               #
#                                                               #
#################################################################

parser = OptionParser()
parser.add_option("-q", "--quiet", action="store_false", dest="verbose", default=False, help="don't print status messages to stdout")
(options, args) = parser.parse_args()
if len(args) == 0:
  usage()

print "sys.argv[0] = " + sys.argv[0]
print "args[0] = " + args[0]

# We might want to iterate over ALL filenames!
filename = args[0]

try:
     f = open(filename, 'r')
except IOError as e:
     print 'Could not open file ', filename
# Extract the basename of the filename, as the path is not of interest after this point
filename = os.path.basename(filename)

def extract_archive_date(filename):
      m = re.match('^(\d{4})(\d{2})(\d{2})', filename)
      return (m.group(1),m.group(2))

# test if archive directory exist, and create it if necessary
def create_archive_dir(ym):
    if (not os.path.exists(ym)):
        os.makedirs(ym)
    
fname = "20120212T000000Z-mlab1-ham01-glasnost-0000.tgz.csv"
(year,month) = extract_archive_date(fname)
create_archive_dir(year +'/'+ month)


sys.exit(1)
for file in args:
	print "filename=" + file
	shutil.move(file,"dest")


