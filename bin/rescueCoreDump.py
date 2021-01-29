
# This file uses apport-retrace infrastructure to read a core dump from an
# apport crash file. It is tied to a particular implementation of apport-retrace.

import os
import sys
import shutil
import tempfile
from apport import Report

# Load crash file as apport "report".
report = Report()
inputFile = open(sys.argv[1], "rb")
report.load(inputFile, binary='compressed')

# Get the args apport wants us to pass to GDB.
coreFile = None
cmdline = report.gdb_command(None)
for arg in cmdline:
    if 'apport_core' in arg:
        coreFile = arg.split(' ')[1]

if not coreFile:
    print "No core dump is present in the provided crash file"
    sys.exit(1)
shutil.copy(coreFile, sys.argv[2])

print "Core file written to %s" % sys.argv[2]
