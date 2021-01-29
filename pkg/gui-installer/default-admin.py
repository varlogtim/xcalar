#!/usr/bin/env python

import json
import os
import argparse
import hmac
import hashlib
import sys

salt = "xcalar-salt"
msg = ""
password = ""
emailAddress = ""
username = ""

parser = argparse.ArgumentParser()

parser.add_argument("-e", "--emailAddress", help="email address of the default admin user")
parser.add_argument("-u", "--userName", help="user name of the default admin user")
parser.add_argument("-p", "--password", help="password of the default admin user")

args = parser.parse_args()

if args.emailAddress:
    emailAddress = args.emailAddress

if args.password:
    password = args.password

if args.userName:
    username = args.userName

# Get python version compatibility via feature detection
try:
    encryptedPassword = hmac.new(
        bytes("xcalar-salt", 'utf-8'),
        bytes(password, 'utf-8'),
        digestmod=hashlib.sha256
    ).hexdigest().lower()
except TypeError:
    encryptedPassword = hmac.new(
        "xcalar-salt",
        password,
        digestmod=hashlib.sha256
    ).hexdigest().lower()

print (json.dumps({"username": username, "password": encryptedPassword, "email": emailAddress, "defaultAdminEnabled": True}))
