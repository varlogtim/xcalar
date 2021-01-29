#! /bin/bash

# sum(votes.funny) = 2545415
# sum(votes.useful) = 4760532
# sum(mult(votes.funny, votes.funny)) = 9694744541
# sum(mult(votes.useful, votes.useful)) = 19098995742
# sum(mult(votes.funny, votes.useful)) = 13066098281

makeCorrelationString()
{
    local arg1=$1
    local arg2=$2
    output="div(sum(mult(sub($arg1, avg($arg1)), sub($arg2, avg($arg2)))), sqrt(mult(sum(pow(sub($arg1, avg($arg1)), 2)), sum(pow(sub($arg2, avg($arg2)), 2)))))"
}

correlationTest()
{
    #depends on basicChecks() table
    local srcTableName="yelp/user-name"

    makeCorrelationString "p::votes.useful" "p::votes.funny"
    local corrString="$output"

    local expectedCorrIndex="0.960231"
    xaggregate "$srcTableName" "$corrString" "$srcTableName-corr-table"
    local corrIndex="$output"
    echo "Correlation between votes.useful and votes.funny: $corrIndex"
    if [ "$corrIndex" != "$expectedCorrIndex" ]; then
        printInColor "red" "Compute corrIndex ($corrIndex) != expectedCorrIndex ($expectedCorrIndex)"
        return 1
    fi

    echo "PASS" 1>&2
    return 0
}

correlation0Test()
{
    local srcTableName="yelp/zero-table"
    #create a column of 0s to obtain a correlation of 0
    xmap "mult(p::votes.funny, 0)" "yelp/user-name" "zeroes" "$srcTableName"
    makeCorrelationString "zeroes" "p::votes.funny"
    local corrString="$output"

    local expectedCorrIndex="Error: server returned: XCE-000000F8 Divide by zero error"
    xaggregate "$srcTableName" "$corrString" "$srcTableName-corr-table"
    local corrIndex="$output"
    echo "Correlation between zeroes and votes.funny: $corrIndex"
    if [ "$corrIndex" != "$expectedCorrIndex" ]; then
        printInColor "red" "Compute corrIndex ($corrIndex) != expectedCorrIndex ($expectedCorrIndex)"
        return 1
    fi

    echo "PASS" 1>&2
    return 0
}
