#!/usr/bin/env python3.6
# Copyright 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#


import string
from random import *
allchar = string.ascii_letters + string.digits
pw = "".join(choice(allchar) for x in range(randint(30,40)))
print (pw)
