#!/usr/bin/env python2.7

import json
import optparse
import subprocess
import os
import glob

parser = optparse.OptionParser(usage="usage: %prog [options]", version="%prog 1.0")

parser.add_option("-p", "--package-file", dest='package',
                  help="nodejs package.json file")
parser.add_option("-d", "--dest-dir", dest='dest',
                  help="destination directory for generated packages")
parser.add_option("-r", "--remove", dest='remove',
                  help="delete contents of --dest-dir first")
parser.add_option("-s", "--system", dest='system',
                  type="choice", choices=['el6', 'el7'],
                  help="operating system platform for deployment");


(options, args) = parser.parse_args()

top_mandatories = [ 'package', 'dest', 'system' ]
for m in top_mandatories:
    if (not options.__dict__[m]):
        print "mandatory option {0} is missing\n".format(m)
        parser.print_help()
        exit(-1)

if (not os.path.isdir(options.dest)):
    print "destination {0} is not a directory".format(options.dest)
    exit(1)

if (not os.path.exists(options.package)):
    print "package file {0} does not exist".format(options.package)
    exit(1)

rm_path = os.path.join(options.dest, "*")
cmd="rm -rf {0}".format(rm_path)
print "Running delete command {0}".format(cmd)
subprocess.check_call(cmd, shell=True)

with open(options.package) as package_file:
    package = json.load(package_file)

npm_prefix = '--npm-package-name-prefix nodejs'

for dep in package["dependencies"]:
    print "Building package for dependency {0}".format(dep)

    if (dep == "socket.io"):
        cmd_template="fpm -s npm -t rpm -n {4} -p {0} -v {1} -d nodejs -a noarch --rpm-dist {3} --iteration 1 {2}"
        cmd = cmd_template.format(options.dest,
                                  package["dependencies"][dep].strip('^'),
                                  dep,
                                  options.system,
                                  "nodejs-socket-io")
    else:
        cmd_template="fpm -s npm -t rpm -p {0} -v {1} -d nodejs -a noarch --rpm-dist {3} {4} --iteration 1 {2}"
        cmd = cmd_template.format(options.dest,
                                  package["dependencies"][dep].strip('^'),
                                  dep,
                                  options.system,
                                  npm_prefix)
    subprocess.check_call(cmd.split())
