# Library of qa-related bash functions

readonly TestCaseEnable=1
readonly TestCaseDisable=0

currentTestNumber=0
totalTestCases=0

declare -A testCaseName
declare -A testCase
declare -A testCaseEnabled
declare -A testCaseWitness

pass()
{
    local testName=$1
    local timeStr=$2
    currentTestNumber=$(( $currentTestNumber + 1 ))
    echo "ok $currentTestNumber - Test \"$testName\" passed in $2"
}

fail()
{
    local testName=$1
    local timeStr=$2
    currentTestNumber=$(( $currentTestNumber + 1 ))
    echo "not ok $currentTestNumber - Test \"$testName\" failed in $2"
}

skip()
{
    local testName=$1
    currentTestNumber=$(( $currentTestNumber + 1 ))
    echo "ok $currentTestNumber - Test \"$testName\" disabled # SKIP"
}

addTestCase()
{
    local tcName=$1
    local tcFunction=$2
    local tcEnabled=$3
    local tcWitness=$4

    testCaseName[$totalTestCases]="$tcName"
    testCase[$totalTestCases]=$tcFunction
    testCaseEnabled[$totalTestCases]=$tcEnabled
    testCaseWitness[$totalTestCases]="$tcWitness"

    totalTestCases=$(( $totalTestCases + 1 ))
}

runTestSuite()
{
    local ii
    local ret

    for ii in `seq 0 $(( $totalTestCases - 1 ))`; do
        local testName=${testCaseName[$ii]}
        local testEnabled=${testCaseEnabled[$ii]}
        local testFn=${testCase[$ii]}
        if [ "$testEnabled" = "$TestCaseEnable" ]; then
            echo -e "\n\nCommencing \"$testName\""
            echo "--------------------------------------"
            local startTime=$(date +%s%N)
            $testFn
            ret=$?
            local endTime=$(date +%s%N)
            local msElapsed=$(($((endTime - startTime))/1000000))
            local formattedTime="$((msElapsed / 1000)).$((msElapsed % 1000))s"
            if [ $ret -eq 0 ]; then
                pass "$testName" "$formattedTime"
            else
                fail "$testName" "$formattedTime"
                forcefulExit $ret
            fi
        else
            echo -e "\n"
            skip "$testName"
        fi
    done
}

emitTap()
{
    local outputFile=$1
    numTestCases=`grep "1\.\." $outputFile | cut -d . -f 3`
    grep "1\.\." $outputFile
    grep "not ok\|ok\|#" $outputFile
}

