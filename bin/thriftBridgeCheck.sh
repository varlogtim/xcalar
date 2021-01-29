#!/bin/bash

DIR=`dirname ${BASH_SOURCE[0]}`
SRCROOT=$DIR/..

echo "1..1"

EXCEPTIONS="XcalarApiUnknown XcalarApiShutdownLocal XcalarApiFunctionInvalid XcalarApiGetPerNodeOpStats \
            XcalarApiErrorpointSet XcalarApiErrorpointList XcalarApiSessionInfo XcalarApiStartNodes \
            XcalarApiMemory"

retval=0
for api in `cat $SRCROOT/src/include/libapis/LibApisEnums.h | grep '    XcalarApi'`
do
    api=`echo $api | sed 's/,//'`
    grep -q $api $SRCROOT/src/bin/legacyJsClient/XcalarApi.js
    if [ "$?" != "0" ]
    then
        echo $EXCEPTIONS | grep -q $api
        if [ "$?" != "0" ]
        then
            echo "*** please implement $api binding in $SRCROOT/src/lib/libapis/XcalarApi.js" 1>&2
            retval=1
        fi
    fi
done

for fn in `grep 'function xcalar' $SRCROOT/src/bin/legacyJsClient/XcalarApi.js | fgrep -v 'WorkItem(' | fgrep -v 'Int(' | cut -f2 -d' ' | cut -f1 -d'('`
do
    grep -q $fn $SRCROOT/src/bin/tests/MgmtTest.js
    if [ "$?" != "0" ]
    then
        echo "*** please implement $fn unit test in $SRCROOT/src/bin/tests/MgmtTest.js" 1>&2
        retval=1
    fi
done

if [ "$retval" = "0" ]; then
    echo "ok 1"
else
    echo "not ok 1"
fi

exit $retval
