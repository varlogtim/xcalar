#!/bin/bash

groupByJoinTest()
{
    local NYCDATA=$BUILD_DIR/src/data/qa/caseStudy/2

    # suffix the population dataset to make it distinct from the case study 2 test use of this dataset
    xccli -c "load --url nfs://$NYCDATA/demographics/population.csv --format csv --fielddelim , --header --name population-GBJT"
    xccli -c "index --dataset .XcalarDS.population-GBJT --key pop_borough --dsttable population-pop_borough"
    xccli -c 'filter --srctable population-pop_borough --eval "like(pop_year,\"2010\")" --dsttable population-pop_borough-2010'
    xccli -c 'map --eval "int(pop_total)" --srctable population-pop_borough-2010 --fieldName pop_total_int --dsttable population-pop_borough-2010-map'
    xccli -c 'groupBy --srctable population-pop_borough-2010-map --eval "sum(pop_total_int)" --fieldName pop_sum --dsttable population-sum-by-borough --nosample'
    xccli -c "join --leftTable  population-pop_borough-2010-map --rightTable population-sum-by-borough --joinTable population-pop_borough-sum"

    local numWhiteInBronx=`xccli -c "cat population-pop_borough-sum 0 6" | fgrep '"pop_race":"W"' | wc -l`

    if [ "$numWhiteInBronx" != "1" ]
    then
        xccli -c "cat population-pop_borough-sum 0 6" 1>&2
        echo "Expecting 1 row with pop_race=W but found $numWhiteInBronx" 1>&2
        return 1
    fi

    xccli -c "drop table population-pop_borough"
    xccli -c "drop table population-pop_borough-2010"
    xccli -c "drop table population-pop_borough-2010-map"
    xccli -c "drop table population-sum-by-borough"
    xccli -c "drop table population-pop_borough-sum"
    xccli -c "drop dataset .XcalarDS.population-GBJT"
    xccli -c "delist .XcalarDS.population-GBJT"
    return 0
}

if [ "$_" = "$0" ]
then
    # run standalone
    groupByJoinTest
    ret=$?
    exit $ret
fi
