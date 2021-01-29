# Copyright 2016 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
import os

#
# This file grabs custom Xcalar configurations from environment variables and determines
# default configuration.
#

# Defaults.
XcalarConfigPath = "/etc/xcalar/default.cfg"

# service endpoints, used when not using proxy
XcalarMgmtdUrl = "http://localhost:9090/thrift/service/XcalarApiService/"
XcalarServiceXceUrl = "http://localhost:12124/service/xce"

# Authentication layer paths
XcalarServiceSessionPath = "/app/auth/serviceSession"
XcalarLoginPath = "/app/login"
XcalarSessionStatusPath = "/app/auth/sessionStatus"

if "XCE_CONFIG" in os.environ:
    XcalarConfigPath = os.environ["XCE_CONFIG"]

if "XLRPY_MGMTD_URL" in os.environ:
    XcalarMgmtdUrl = os.environ["XLRPY_MGMTD_URL"]

if "XLRPY_SERVICE_URL" in os.environ:
    XcalarServiceXceUrl = os.environ["XLRPY_SERVICE_URL"]
