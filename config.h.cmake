// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#cmakedefine HAVE_ARCHIVE_H @HAVE_ARCHIVE_H@

#cmakedefine HAVE_ARCHIVE3_H @HAVE_ARCHIVE3_H@

#cmakedefine PREFIX "@PREFIX@"

#cmakedefine SIZEOF_DOUBLE @SIZEOF_DOUBLE@

#cmakedefine SIZEOF_FD_SET @SIZEOF_FD_SET@

#cmakedefine SIZEOF_FLOAT @SIZEOF_FLOAT@

#cmakedefine SIZEOF_INT @SIZEOF_INT@

#cmakedefine SIZEOF_LONG_DOUBLE @SIZEOF_LONG_DOUBLE@

#cmakedefine SIZEOF_LONG_INT @SIZEOF_LONG_INT@

// XXX - Determine this dynamically
#define SIZEOF_STRUCT_IOVEC 16

// XXX - Determine this dynamically
#define SIZEOF_STRUCT_SOCKADDR_IN 16

// XXX - Determine this dynamically
#define SIZEOF_VA_LIST 24
