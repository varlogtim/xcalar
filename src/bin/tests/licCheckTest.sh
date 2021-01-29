#!/bin/bash

DIR=`dirname ${BASH_SOURCE[0]}`

echo "*** licCheckTest.sh STARTING ON `/bin/date`"

pathToKey="$XLRDIR/src/bin/tests"

checkLicense () {
        testName=$1
        licenseFile=$2
        expectedCode=$3
        expectedMsg=$4

        echo "License file name: $licenseFile"

        echo "Running test $testName: "

        retMsg=`licenseCheck $pathToKey/EcdsaPub.key $licenseFile 2>&1`
        retCode=$?

        if [ $retCode -ne $expectedCode ]; then
                echo "FAIL" 1>&2
                echo "Return code $retCode was unexpected. Exiting..."
                return 1
        fi

        echo "PASS" 1>&2
        return 0
}

licenseGood () {
        checkLicense "License Good" "$pathToKey/XcalarLic.key" 0
        return $?
}

md5Failure () {
        checkLicense "MD5 failure" "$pathToKey/XcalarLic.key.bad" 1
        return $?
}

expiredFailure () {
        # Xc-7112 - the expiration time of the license is stored as a month/day/year date, not
        # a full epoch timestamp.  The routine that converts that date back to an epoch time tries
        # to detect if daylight savings time applies.  The output routine then uses a localtime
        # call that converts to the correct local wall clock time (taking dst into account.)
        # This message should therefore be constant.
        checkLicense "Expired failure" "$pathToKey/XcalarLic.key.expired" 1
        return $?
}

badProduct () {
        checkLicense "Bad product" "$pathToKey/XcalarLic.key.product" 1
        return $?
}

forcefulExit () {
        exitCode=$1
        echo "Error detected."
    echo "Completed 1..$currentTestNumber"
    exit $exitCode
}

userBreakpoint()
{
    msg=$1
    if [ "$interactiveMode" != "" -a "$interactiveMode" = "1" ]; then
        # gdball.sh
        echo "$msg Press enter to continue"
        read
    fi
}

. utils.sh
. qa.sh

addTestCase "License Good" licenseGood $TestCaseEnable ""
addTestCase "MD5 failure" md5Failure $TestCaseEnable ""
addTestCase "Expired failure" expiredFailure $TestCaseEnable ""
addTestCase "Bad product" badProduct $TestCaseEnable ""

userBreakpoint "All nodes ready"
runTestSuite

printInColor "green" "All tests succeeded"

echo "1..$currentTestNumber"
echo "*** licCheckTest.sh ENDING ON `/bin/date`"
