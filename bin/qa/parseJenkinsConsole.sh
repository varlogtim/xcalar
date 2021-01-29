#! /bin/bash

if [ $# -ne 5 ]; then
    echo "Usage $0 <url> <dburl> <dbuser> <dbpass> <gitCommitSha>"
    exit 1
fi

url=$1
dburl=$2
db="xcalar-qa"
testCasesTable="testCases"
testRunTable="testRuns"
testResultTable="testResults"
dbuser=$3
dbpass=$4
gitCommit=$5

escape()
{
    local value=$1
    escapeValue=`echo "$value" | sed 's/\"/\\\"/g'`
}

newTestRun()
{
    escape "$gitCommit"
    local gitCommitSha="$escapeValue"
    local query='INSERT INTO '$testRunTable' (gitCommit) VALUES ("'$gitCommitSha'")'
    resultSet=`
mysql -h "$dburl" -u "$dbuser" -p"$dbpass" << EOF
use $db
$query;
SELECT LAST_INSERT_ID()
EOF`
    testRunId=`echo "$resultSet" | tail -n +2`
}

addTestCase()
{
    local testName=$1
    escape "$testName"
    testName="$escapeValue"

    local query='INSERT INTO '$testCasesTable' (testName) VALUES ("'$testName'")'
mysql -h "$dburl" -u "$dbuser" -p"$dbpass" << EOF
use $db
$query
EOF

    testCaseExists "$1"
}

testCaseExists()
{
    local testName=$1
    escape "$testName"
    testName="$escapeValue"
    local query='SELECT testCaseId FROM '$testCasesTable' WHERE testName="'$testName'"'
    resultSet=`
mysql -h "$dburl" -u "$dbuser" -p"$dbpass" << EOF
use $db
$query
EOF`
    numLines=`echo "$resultSet" | wc -l`
    if [ "$numLines" -eq 1 ]; then
        return 1
    fi

    testCaseId=`echo "$resultSet" | tail -n +2`
    return 0
}

reportTestResult()
{
    local testRunId=$1
    local testCaseId=$2
    local testStatus=$3
    escape "$testStatus"
    testStatus="$escapeValue"
    local testComment=$4
    escape "$testComment"
    testComment="$escapeValue"

    local query='INSERT INTO '$testResultTable' (testCaseId, testRunId, status, comments) VALUES ('$testCaseId', '$testRunId', '"'"$testStatus"'"', '"\""$testComment"\""')'
mysql -h "$dburl" -u "$dbuser" -p"$dbpass" << EOF
use $db
$query
EOF
}

newTestRun
echo "Test run: $testRunId"

fullLog=`curl $1`
listOfTests=`echo "$fullLog" | grep "^PASS: \|^FAIL: \|^SKIP: "`
while read test
do
    oldIFS=$IFS
    IFS=':'
    read -r testStatus testString <<< "$test"

    IFS='-'
    read -r testFileName testComment <<< "$testString"
    IFS=$oldIFS

    testFileName=`echo "$testFileName" | cut -d \  -f 2`
    testComment=`echo "$testComment" | sed 's/^ *//'`
    echo "$testComment" | grep -q "^Test"
    ret=$?
    if [ $ret -ne 0 ]; then
        continue
    fi

    testName=`echo "$testComment" | grep -Po '".*?"'`
    testFullName="$testFileName-$testName"

    testCaseExists "$testFullName"
    ret=$?

    if [ $ret -ne 0 ]; then
        echo "Adding test: $testFullName"
        addTestCase "$testFullName"
    fi

    reportTestResult "$testRunId" "$testCaseId" "$testStatus" "$testComment"
done <<< "$listOfTests"
