// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _TESTQUERIES_H_
#define _TESTQUERIES_H_

// Query used by Customer 1

// This format is used as input to the format argument in the following
// query.  This is needed as we want the resultant formated query to
// include formatting specifications itself.
//
static constexpr const char *cust1QueryFormat1 = "'%m/%d/%y', '%y%m'";

// Size of buffer needed to hold longest of these queries.  This is
// checked by the queryparser func test (qpDoParseTest).
static const size_t MaxTestQueryBufSize = 16 * KB;

static constexpr const char *cust1Query =
    "load --url "
    "nfs:///netstore/users/ssperson/POCs/Customer1/gb_exp_100K.csv "
    "--format csv --size 0B --name Customer1.gbexp100K.%s "
    "--fielddelim , --crlf --header;"
    "load --url nfs:///netstore/users/ssperson/POCs/Customer1/"
    "ipdr_exp_500K.csv --format csv --size 0B "
    "--name Customer1.ipdrexp500K.%s "
    "--fielddelim , --crlf --header;"

    "index --key xcalarRecordNum --dataset "
    ".XcalarDS.Customer1.gbexp100K.%s "
    "--dsttable gbexp100K#jh0 --prefix gb;"
    "index --key xcalarRecordNum --dataset "
    ".XcalarDS.Cusomter1.ipdrexp500K.%s "
    "--dsttable ipdrexp500K#jh1 --prefix ip;"
    "map --eval int(gb::STB_CAPABLE) --srctable gbexp100K#jh0 "
    "--fieldName STB_CAPABLE_integer --dsttable gbexp100K#jh2;"
    "map --eval int(gb::NUM_HOURS) --srctable gbexp100K#jh2 "
    "--fieldName NUM_HOURS_integer --dsttable gbexp100K#jh3;"
    "map --eval float(gb::KBPS_UP) --srctable gbexp100K#jh3 --fieldName "
    "KBPS_UP_float "
    "--dsttable gbexp100K#jh4;"
    "map --eval float(gb::KBPS_DOWN) --srctable gbexp100K#jh4 "
    "--fieldName KBPS_DOWN_float --dsttable gbexp100K#jh5;"
    "map --eval int(gb::MAC) --srctable gbexp100K#jh5 --fieldName MAC_integer "
    "--dsttable gbexp100K#jh6;"
    "map --eval \"add(KBPS_DOWN_float, KBPS_UP_float)\" --srctable "
    "gbexp100K#jh6 "
    "--fieldName KBPS_DOWN_float_add6 --dsttable gbexp100K#jh7;"
    "index --key KBPS_DOWN_float_add6 --srctable gbexp100K#jh7 "
    "--dsttable gbexp100K#jh8 --sorted;"
    "aggregate --srctable gbexp100K#jh7 --dsttable gbexp100K-aggregate#jh9, "
    "--eval min(KBPS_DOWN_float_add6);"
    "aggregate --srctable gbexp100K#jh7 --dsttable gbexp100K-aggregate#jh10, "
    "--eval avg(KBPS_DOWN_float_add6);"
    "aggregate --srctable gbexp100K#jh7 --dsttable gbexp100K-aggregate#jh11, "
    "--eval max(KBPS_DOWN_float_add6);"
    "aggregate --srctable gbexp100K#jh7 --dsttable gbexp100K-aggregate#jh12, "
    "--eval count(KBPS_DOWN_float_add6);"
    "aggregate --srctable gbexp100K#jh7 --dsttable gbexp100K-aggregate#jh13, "
    "--eval sum(KBPS_DOWN_float_add6);"
    "index --key KBPS_DOWN_float_add6 --srctable gbexp100K#jh7 "
    "--dsttable gbexp100K.indexParent65426#jh14;"
    "groupBy --srctable gbexp100K.indexParent65426#jh14 "
    "--eval count(KBPS_DOWN_float_add6) --fieldName statsGroupBy "
    "--dsttable gbexp100K.profile.GB71151#jh15 --nosample;"
    "index --key KBPS_DOWN_float_add6 --srctable "
    "gbexp100K.profile.GB71151#jh15 "
    "--dsttable gbexp100K.profile.final35862#jh16 --sorted;"
    "aggregate --srctable gbexp100K.profile.GB71151#jh15 "
    "--dsttable gbexp100K.profile.final35862-aggregate#jh17, "
    "--eval max(statsGroupBy);"
    "aggregate --srctable gbexp100K.profile.GB71151#jh15 "
    "--dsttable gbexp100K.profile.final35862-aggregate#jh18, "
    "--eval sum(statsGroupBy);"
    "drop table gbexp100K.indexParent65426#jh14;"
    "map --eval \"mult(KBPS_DOWN_float_add6, 3600)\" --srctable gbexp100K#jh7 "
    "--fieldName KBPS_DOWN_float_add6_mult8 --dsttable gbexp100K#jh19;"
    "index --key KBPS_DOWN_float_add6_mult8 --srctable gbexp100K#jh19 "
    "--dsttable gbexp100K#jh20 --sorted;"
    "aggregate --srctable gbexp100K#jh19 --dsttable gbexp100K-aggregate#jh21, "
    "--eval min(KBPS_DOWN_float_add6_mult8);"
    "aggregate --srctable gbexp100K#jh19 --dsttable gbexp100K-aggregate#jh22, "
    "--eval avg(KBPS_DOWN_float_add6_mult8);"
    "aggregate --srctable gbexp100K#jh19 --dsttable gbexp100K-aggregate#jh23, "
    "--eval max(KBPS_DOWN_float_add6_mult8);"
    "aggregate --srctable gbexp100K#jh19 --dsttable gbexp100K-aggregate#jh24, "
    "--eval count(KBPS_DOWN_float_add6_mult8);"
    "aggregate --srctable gbexp100K#jh19 --dsttable gbexp100K-aggregate#jh25, "
    "--eval sum(KBPS_DOWN_float_add6_mult8);"
    "index --key KBPS_DOWN_float_add6_mult8 --srctable gbexp100K#jh19 "
    "--dsttable gbexp100K.indexParent25957#jh26;"
    "groupBy --srctable gbexp100K.indexParent25957#jh26 "
    "--eval count(KBPS_DOWN_float_add6_mult8) --fieldName statsGroupBy "
    "--dsttable gbexp100K.profile.GB29778#jh27 --nosample;"
    "index --key KBPS_DOWN_float_add6_mult8 "
    "--srctable gbexp100K.profile.GB29778#jh27 "
    "--dsttable gbexp100K.profile.final25981#jh28 --sorted;"
    "aggregate --srctable gbexp100K.profile.GB29778#jh27 "
    "--dsttable gbexp100K.profile.final25981-aggregate#jh29, "
    "--eval max(statsGroupBy);"
    "aggregate --srctable gbexp100K.profile.GB29778#jh27 "
    "--dsttable gbexp100K.profile.final25981-aggregate#jh30, "
    "--eval sum(statsGroupBy);"
    "drop table gbexp100K.indexParent25957#jh26;"
    "map --eval \"div(KBPS_DOWN_float_add6_mult8, 1048576)\" "
    "--srctable gbexp100K#jh19 --fieldName KBPS_DOWN_float_add6_mult8_div20 "
    "--dsttable gbexp100K#jh31;"
    "aggregate --srctable gbexp100K#jh31 --dsttable gbexp100K-aggregate#jh32, "
    "--eval \"min(KBPS_DOWN_float_add6_mult8_div20)\";"
    "aggregate --srctable gbexp100K#jh31 --dsttable gbexp100K-aggregate#jh34, "
    "--eval \"max(KBPS_DOWN_float_add6_mult8_div20)\";"
    "aggregate --srctable gbexp100K#jh31 --dsttable gbexp100K-aggregate#jh33, "
    "--eval \"avg(KBPS_DOWN_float_add6_mult8_div20)\";"
    "aggregate --srctable gbexp100K#jh31 --dsttable gbexp100K-aggregate#jh35, "
    "--eval \"count(KBPS_DOWN_float_add6_mult8_div20)\";"
    "aggregate --srctable gbexp100K#jh31 --dsttable gbexp100K-aggregate#jh36, "
    "--eval \"sum(KBPS_DOWN_float_add6_mult8_div20)\";"
    "index --key KBPS_DOWN_float_add6_mult8_div20 --srctable gbexp100K#jh31 "
    "--dsttable gbexp100K.index#jh37;"
    "aggregate --srctable gbexp100K.index#jh37 "
    "--dsttable gbexp100K.index-aggregate#jh38, "
    "--eval \"count(KBPS_DOWN_float_add6_mult8_div20)\";"
    "groupBy --srctable gbexp100K.index#jh37 "
    "--eval \"count(KBPS_DOWN_float_add6_mult8_div20)\" "
    "--fieldName statsGroupBy --dsttable gbexp100K.profile.GB53763#jh39 "
    "--nosample;"
    "index --key KBPS_DOWN_float_add6_mult8_div20 "
    "--srctable gbexp100K.profile.GB53763#jh39 "
    "--dsttable gbexp100K.profile.final66173#jh40 --sorted;"
    "aggregate --srctable gbexp100K.profile.GB53763#jh39 "
    "--dsttable gbexp100K.profile.final66173-aggregate#jh41, "
    "--eval \"max(statsGroupBy)\";"
    "aggregate --srctable gbexp100K.profile.GB53763#jh39 "
    "--dsttable gbexp100K.profile.final66173-aggregate#jh42, "
    "--eval \"sum(statsGroupBy)\";"
    "drop table gbexp100K.index#jh37;"
    "map --eval \"mult(floor(div(KBPS_DOWN_float_add6_mult8_div20, 50)), 50)\" "
    "--srctable gbexp100K.profile.GB53763#jh39 --fieldName bucketMap4183 "
    "--dsttable gbexp100K.profile.final66173.bucket#jh43;"
    "index --key bucketMap4183 --srctable "
    "gbexp100K.profile.final66173.bucket#jh43 "
    "--dsttable gbexp100K.profile.final66173.bucket.index47593#jh44;"
    "groupBy --srctable gbexp100K.profile.final66173.bucket.index47593#jh44 "
    "--eval \"sum(statsGroupBy)\" --fieldName bucketGroupBy "
    "--dsttable gbexp100K.profile.final66173.bucket.groupby99942#jh45 "
    "--nosample;"
    "index --key bucketMap4183 "
    "--srctable gbexp100K.profile.final66173.bucket.groupby99942#jh45 "
    "--dsttable gbexp100K.profile.final66173.bucket.final85342#jh46 --sorted;"
    "aggregate --srctable "
    "gbexp100K.profile.final66173.bucket.groupby99942#jh45 "
    "--dsttable gbexp100K.profile.final66173.bucket.final85342-aggregate#jh47, "
    "--eval \"max(bucketGroupBy)\";"
    "aggregate --srctable "
    "gbexp100K.profile.final66173.bucket.groupby99942#jh45 "
    "--dsttable gbexp100K.profile.final66173.bucket.final85342-aggregate#jh48, "
    "--eval \"sum(bucketGroupBy)\";"
    "drop table gbexp100K.profile.final66173.bucket#jh43;"
    "drop table gbexp100K.profile.final66173.bucket.index47593#jh44;"

    // "map --eval \"default:convertFormats(MONTH_FROM, '%m/%d/%y', '%y%m')\" "
    // The below %s gets filled in with the above formatting.  This is neeeded
    // to include formatting withing a string that itself is formatted.  See
    // cust1QueryFormat1 up above.
    "map --eval \"default:convertFormats(MONTH_FROM, %s)\" "
    "--srctable gbexp100K#jh31 --fieldName MONTH_FROM_udf31 "
    "--dsttable gbexp100K#jh52;"

    "index --key MONTH_FROM_udf31 --srctable gbexp100K#jh52 "
    "--dsttable gbexp100K.index#jh53;"
    "groupBy --srctable gbexp100K.index#jh53 "
    "--eval sum(KBPS_DOWN_float_add6_mult8_div20) "
    "--fieldName MONTH_FROM_udf31_sum52 "
    "--dsttable gbexp100K-GB#jh54 --nosample;"
    "index --key MAC_integer --srctable gbexp100K#jh52 "
    "--dsttable gbexp100K.index#jh55;"
    "groupBy --srctable gbexp100K.index#jh55 "
    "--eval sum(KBPS_DOWN_float_add6_mult8_div20) "
    "--fieldName MAC_integer_sum52 --dsttable gbexp100K-GB#jh56 "
    "--nosample;"
    "index --key ServiceClassName --srctable ipdrexp500K#jh1 "
    "--dsttable .tempIndex.ipdrexp500K#jh57;"
    "groupBy --srctable .tempIndex.ipdrexp500K#jh57 --eval "
    "count(ServiceClassName) "
    "--fieldName randCol60131 --dsttable .tempGB.ipdrexp500K#jh58 --nosample;"
    "index --key randCol60131 --srctable .tempGB.ipdrexp500K#jh58 "
    "--dsttable .tempGB-Sort.ipdrexp500K#jh59 --sorted=desc;"
    "filter --srctable ipdrexp500K#jh1 "
    "--eval \"eq(ServiceClassName, 'sc_ds_pub_wifi')\" "
    "--dsttable ipdrexp500K-HP1#jh60;"
    "filter --srctable ipdrexp500K#jh1 "
    "--eval \"eq(ServiceClassName, 'sc_us_pub_stb')\" "
    "--dsttable ipdrexp500K-HP2#jh61;"
    "filter --srctable ipdrexp500K#jh1 "
    "--eval \"eq(ServiceClassName, 'sc_us_pub_wifi')\" "
    "--dsttable ipdrexp500K-HP3#jh62;"
    "filter --srctable ipdrexp500K#jh1 "
    "--eval \"eq(ServiceClassName, 'sc_us_pub_voip')\" "
    "--dsttable ipdrexp500K-HP4#jh63;"

    "map --eval \"cut(CmLastRegTime, 1, '-')\" --srctable ipdrexp500K-HP1#jh60 "
    "--fieldName CmLastRegTime-split-1 --dsttable ipdrexp500K-HP1#jh64;"
    "map --eval \"cut(CmLastRegTime, 2, '\')\" --srctable ipdrexp500K-HP1#jh64 "
    "--fieldName CmLastRegTime-split-2 --dsttable ipdrexp500K-HP1#jh65;"

    "map --eval \"default:splitWithDelim(CmLastRegTime, 2, '-')\" "
    "--srctable ipdrexp500K-HP1#jh65 --fieldName CmLastRegTime-split-rest "
    "--dsttable ipdrexp500K-HP1#jh66;"
    "map --eval \"substring(CmLastRegTime-split-1, 2, 0)\" "
    "--srctable ipdrexp500K-HP1#jh66 "
    "--fieldName CmLastRegTime-split-1_substring66 "
    "--dsttable ipdrexp500K-HP1#jh67;"
    "map --eval \"concat(CmLastRegTime-split-1_substring66, "
    "CmLastRegTime-split-2)\" --srctable ipdrexp500K-HP1#jh67 "
    "--fieldName CmLastRegTime-split-1_substring66_concat67 "
    "--dsttable ipdrexp500K-HP1#jh68;"
    "index --key CmLastRegTime-split-1_substring66_concat67 "
    "--srctable ipdrexp500K-HP1#jh68 --dsttable ipdrexp500K-HP1.index#jh70;"
    "join --leftTable ipdrexp500K-HP1.index#jh70 --rightTable "
    "gbexp100K-GB#jh54 "
    "--joinType innerJoin --joinTable joinMonth#jh69;"
    "map --eval \"substring(CmMacAddr, 8, 0)\" "
    "--srctable ipdrexp500K-HP1.index#jh70 --fieldName CmMacAddr_substring70 "
    "--dsttable ipdrexp500K-HP1.index#jh71;"

    // Need to figure out how to handle the empty token ''

    // "map --eval replace(CmMacAddr_substring70, '-', '') "
    //    "--srctable ipdrexp500K-HP1.index#jh71 "
    //    "--fieldName CmMacAddr_substring70_replace71 "
    //    "--dsttable ipdrexp500K-HP1.index#jh72;"
    // "map --eval replace(CmMacAddr_substring70_replace71, '', ':') "
    //     "--srctable ipdrexp500K-HP1.index#jh72 "
    //     "--fieldName CmMacAddr_substring70_replace71_replace72 "
    //     "--dsttable ipdrexp500K-HP1.index#jh73;"

    "map --eval \"countChar(CmMacAddr_substring70_replace71_replace72, ':')\" "
    "--srctable ipdrexp500K-HP1.index#jh71 --fieldName mappedCol80015 "
    "--dsttable .tempMap.ipdrexp500K-HP1.index#jh75;"
    "aggregate --srctable .tempMap.ipdrexp500K-HP1.index#jh75 "
    "--dsttable .tempMap.ipdrexp500K-HP1.index-aggregate#jh76, "
    "--eval maxInteger(mappedCol80015);"
    "map --eval \"cut(CmMacAddr_substring70_replace71_replace72, 1, ':')\" "
    "--srctable ipdrexp500K-HP1.index#jh71 "
    "--fieldName CmMacAddr_substring70_replace71_replace72-split-1 "
    "--dsttable ipdrexp500K-HP1.index#jh77;"
    "map --eval \"cut(CmMacAddr_substring70_replace71_replace72, 2, ':')\" "
    "--srctable ipdrexp500K-HP1.index#jh77 "
    "--fieldName CmMacAddr_substring70_replace71_replace72-split-2 "
    "--dsttable ipdrexp500K-HP1.index#jh78;"
    "map --eval \"cut(CmMacAddr_substring70_replace71_replace72, 3, ':')\" "
    "--srctable ipdrexp500K-HP1.index#jh78 "
    "--fieldName CmMacAddr_substring70_replace71_replace72-split-3 "
    "--dsttable ipdrexp500K-HP1.index#jh79;"
    "map --eval \"cut(CmMacAddr_substring70_replace71_replace72, 4, ':')\" "
    "--srctable ipdrexp500K-HP1.index#jh79 "
    "--fieldName CmMacAddr_substring70_replace71_replace72-split-4 "
    "--dsttable ipdrexp500K-HP1.index#jh80;"
    "map --eval \"cut(CmMacAddr_substring70_replace71_replace72, 5, ':')\" "
    "--srctable ipdrexp500K-HP1.index#jh80 "
    "--fieldName CmMacAddr_substring70_replace71_replace72-split-5 "
    "--dsttable ipdrexp500K-HP1.index#jh81;"
    "map --eval \"cut(CmMacAddr_substring70_replace71_replace72, 6, ':')\" "
    "--srctable ipdrexp500K-HP1.index#jh81 "
    "--fieldName CmMacAddr_substring70_replace71_replace72-split-6 "
    "--dsttable ipdrexp500K-HP1.index#jh82;"
    "map --eval \"cut(CmMacAddr_substring70_replace71_replace72, 7, ':')\" "
    "--srctable ipdrexp500K-HP1.index#jh82 "
    "--fieldName CmMacAddr_substring70_replace71_replace72-split-7 "
    "--dsttable ipdrexp500K-HP1.index#jh83;"
    "map --eval \"concat(CmMacAddr_substring70_replace71_replace72-split-1, "
    "CmMacAddr_substring70_replace71_replace72-split-2)\" "
    "--srctable ipdrexp500K-HP1.index#jh83 "
    "--fieldName CmMacAddr_substring70_replace71_replace72-split-1_concat83 "
    "--dsttable ipdrexp500K-HP1.index#jh84;"
    "map --eval \"concat("
    "CmMacAddr_substring70_replace71_replace72-split-1_concat83, "
    "CmMacAddr_substring70_replace71_replace72-split-3)\" "
    "--srctable ipdrexp500K-HP1.index#jh84 "
    "--fieldName "
    "CmMacAddr_substring70_replace71_replace72-split-1_concat83_concat84 "
    "--dsttable ipdrexp500K-HP1.index#jh85;"
    "map --eval \"concat("
    "CmMacAddr_substring70_replace71_replace72-split-1_concat83_concat84, "
    "CmMacAddr_substring70_replace71_replace72-split-4)\" "
    "--srctable ipdrexp500K-HP1.index#jh85 "
    "--fieldName "
    "CmMacAddr_substring70_replace71_replace72-split-1_concat83_concat84_"
    "concat85 "
    "--dsttable ipdrexp500K-HP1.index#jh86;"
    "map --eval \"concat("
    "CmMacAddr_substring70_replace71_replace72-split-1_concat83_concat84_"
    "concat85, CmMacAddr_substring70_replace71_replace72-split-5)\" "
    "--srctable ipdrexp500K-HP1.index#jh86 "
    "--fieldName CmMacAddr_substring70_replace71_replace72-split-1_concat83_"
    "concat84_concat85_concat86 --dsttable ipdrexp500K-HP1.index#jh87;"
    "map --eval \"concat(CmMacAddr_substring70_replace71_replace72-split-1_"
    "concat83_concat84_concat85_concat86, "
    "CmMacAddr_substring70_replace71_replace72-split-5)\" "
    "--srctable ipdrexp500K-HP1.index#jh87 "
    "--fieldName CmMacAddr_substring70_replace71_replace72-split-1_concat83_"
    "concat84_concat85_concat86_concat87 --dsttable ipdrexp500K-HP1.index#jh88;"

    "map --eval \"concat(CmMacAddr_substring70_replace71_replace72-split-1_"
    "concat83_concat84_concat85_concat86, CmMacAddr_substring70_replace71_"
    "replace72-split-6)\" --srctable ipdrexp500K-HP1.index#jh88 "
    "--fieldName CmMacAddr_substring70_replace71_replace72-split-1_concat83_"
    "concat84_concat85_concat86_concat88 --dsttable ipdrexp500K-HP1.index#jh89;"
    "map --eval "
    "\"int(CmMacAddr_substring70_replace71_replace72-split-1_concat83_"
    "concat84_concat85_concat86_concat88)\" "
    "--srctable ipdrexp500K-HP1.index#jh89 "
    "--fieldName CmMacAddr_substring70_replace71_replace72-split-1_concat83_"
    "concat84_concat85_concat86_concat88_integer "
    "--dsttable ipdrexp500K-HP1.index#jh90;"
    "index --key CmMacAddr_substring70_replace71_replace72-split-1_concat83_"
    "concat84_concat85_concat86_concat88_integer "
    "--srctable ipdrexp500K-HP1.index#jh90 "
    "--dsttable ipdrexp500K-HP1.index.index#jh92;"
    "join --leftTable ipdrexp500K-HP1.index.index#jh92 "
    "--rightTable gbexp100K-GB#jh56 --joinType innerJoin "
    "--joinTable joinMac#jh91;"
    "drop table *; drop constant *; drop dataset *;";

// Query developed while working on Customer 2

static constexpr const char *cust2Query =
    "load --url nfs:///netstore/datasets/Customer2BBO/NYSE/BBO_8000 "
    "--format csv --size 0B  --name customer2.BBO8000.%s "
    "--fielddelim , --crlf --header --apply customer2:fixed_to_CSV;"
    "index --key xcalarRecordNum --dataset .XcalarDS.customer2.BBO8000.%s "
    "--dsttable BBO8000#ui0 --prefix p;"
    "map --eval \"int(AskSize, 10)\" --srctable BBO8000#ui0 "
    "--fieldName AskSize_integer --dsttable BBO8000#ui1;"
    "map --eval \"int(AskPrice, 10)\" --srctable BBO8000#ui1 "
    "--fieldName AskPrice_integer --dsttable BBO8000#ui2;"
    "map --eval \"int(BidSize, 10)\" --srctable BBO8000#ui2 "
    "--fieldName BidSize_integer --dsttable BBO8000#ui3;"
    "map --eval \"int(BidPrice, 10)\" --srctable BBO8000#ui3 "
    "--fieldName BidPrice_integer --dsttable BBO8000#ui4;"
    "map --eval \"div(BidPrice_integer, 10000)\" --srctable BBO8000#ui4 "
    "--fieldName BidPrice_ --dsttable BBO8000#ui5;"
    "map --eval \"div(AskPrice_integer, 10000)\" --srctable BBO8000#ui5 "
    "--fieldName AskPrice_ --dsttable BBO8000#ui6;"
    "map --eval \"sub(AskPrice_, BidPrice_)\" --srctable BBO8000#ui6 "
    "--fieldName Price_Difference --dsttable BBO8000#ui7;"
    "index --key Price_Difference --srctable BBO8000#ui7 "
    "--dsttable BBO8000#ui8 --sorted --prefix ;"
    "drop table *; drop constant *; drop dataset *;";

// Flight demo query

static constexpr const char *fdQuery =
    "load --url nfs:///netstore/datasets/flight/airports.csv --format csv "
    "--size 0B --name xxx.airport3614.%s --fielddelim , --crlf --header;"
    "load --url nfs:///netstore/datasets/flight/airlines_2007.csv "
    "--format csv --size 0B --name xxx.flight5205.%s --fielddelim , --crlf "
    "--header;"
    "index --key xcalarRecordNum --dataset .XcalarDS.xxx.flight5205.%s "
    "--dsttable flight5205#bt0 --prefix f;"
    "index --key xcalarRecordNum --dataset .XcalarDS.xxx.airport3614.%s "
    "--dsttable airport3614#bt1 --prefix a;"
    "map --eval \"int(ArrDelay)\" --srctable flight5205#bt0 "
    "--fieldName ArrDelay_integer --dsttable flight5205#bt2;"
    "map --eval \"genUnique()\" --srctable flight5205#bt2 --fieldName "
    "uniqueNum "
    "--dsttable flight5205#bt3;"
    "filter --srctable flight5205#bt3 --eval \"gt(ArrDelay_integer, 0)\" "
    "--dsttable flight5205#bt4;"
    // No ymd support in this test
    // "map --eval \"ymd:ymd(Year, Month, DayofMonth)\" --srctable
    // flight5205#bt4 "
    //    "--fieldName YearMonthDay --dsttable flight5205#bt5;"
    "index --key Dest --srctable flight5205#bt4 --dsttable "
    "flight5205.index#bt7;"
    "index --key iata --srctable airport3614#bt1 --dsttable "
    "airport3614.index#bt8;"
    "join --leftTable flight5205.index#bt7 --rightTable airport3614.index#bt8 "
    "--joinType innerJoin --joinTable flight5205-airport3614#bt6;"
    "index --key UniqueCarrier --srctable flight5205-airport3614#bt6 "
    "--dsttable flight5205-airport3614.index#bt9;"
    "groupBy --srctable flight5205-airport3614.index#bt9 "
    "--eval \"avg(ArrDelay_integer)\" --fieldName AvgDelay "
    "--dsttable flight5205-airport3614-GB#bt10 --nosample;"
    "drop table *; drop constant *; drop dataset *;";

// Another Customer 1 query

static constexpr const char *cust1_2Query =
    "load --url nfs:///netstore/datasets/cust1_generated/ad_data.json --format "
    "json "
    "--size 0B  --name asdf3.addata1.%s "
    "--apply default:convertNewLineJsonToArrayJson;"
    "index --key xcalarRecordNum --dataset .XcalarDS.asdf3.addata1.%s "
    "--dsttable addata#6c0 --prefix p;"
    "map --eval \"genUnique()\" --srctable addata#6c0 --fieldName c0 "
    "--dsttable addata#6c1;"
    "map --eval \"genUnique()\" --srctable addata#6c1 --fieldName c2 "
    "--dsttable addata#6c2;"
    "map --eval \"genUnique()\" --srctable addata#6c2 --fieldName c3 "
    "--dsttable addata#6c3;"
    "map --eval \"genUnique()\" --srctable addata#6c3 --fieldName c4 "
    "--dsttable addata#6c4;"
    "map --eval \"genUnique()\" --srctable addata#6c4 --fieldName c5 "
    "--dsttable addata#6c5;"
    "map --eval \"genUnique()\" --srctable addata#6c5 --fieldName c6 "
    "--dsttable addata#6c6;"
    "map --eval \"genUnique()\" --srctable addata#6c6 --fieldName c7 "
    "--dsttable addata#6c7;"
    "map --eval \"genUnique()\" --srctable addata#6c7 --fieldName c8 "
    "--dsttable addata#6c8;"
    "map --eval \"genUnique()\" --srctable addata#6c8 --fieldName c9 "
    "--dsttable addata#6c9;"
    "map --eval \"genUnique()\" --srctable addata#6c9 --fieldName c10 "
    "--dsttable addata#6c10;"
    "map --eval \"genUnique()\" --srctable addata#6c10 --fieldName c11 "
    "--dsttable addata#6c11;"
    "map --eval \"genUnique()\" --srctable addata#6c11 --fieldName c12 "
    "--dsttable addata#6c12;"
    "map --eval \"genUnique()\" --srctable addata#6c12 --fieldName c13 "
    "--dsttable addata#6c13;"
    "map --eval \"genUnique()\" --srctable addata#6c13 --fieldName c14 "
    "--dsttable addata#6c14;"
    "map --eval \"genUnique()\" --srctable addata#6c14 --fieldName c15 "
    "--dsttable addata#6c15;"
    "map --eval \"genUnique()\" --srctable addata#6c15 --fieldName c16 "
    "--dsttable addata#6c16;"
    "map --eval \"genUnique()\" --srctable addata#6c16 --fieldName c17 "
    "--dsttable addata#6c17;"
    "map --eval \"genUnique()\" --srctable addata#6c17 --fieldName c18 "
    "--dsttable addata#6c18;"
    "map --eval \"genUnique()\" --srctable addata#6c18 --fieldName c19 "
    "--dsttable addata#6c19;"
    "map --eval \"genUnique()\" --srctable addata#6c19 --fieldName c20 "
    "--dsttable addata#6c20;"
    "map --eval \"genUnique()\" --srctable addata#6c20 --fieldName c21 "
    "--dsttable addata#6c21;"
    "map --eval \"genUnique()\" --srctable addata#6c21 --fieldName c22 "
    "--dsttable addata#6c22;"
    "map --eval \"genUnique()\" --srctable addata#6c22 --fieldName c23 "
    "--dsttable addata#6c23;"
    "map --eval \"genUnique()\" --srctable addata#6c23 --fieldName c24 "
    "--dsttable addata#6c24;"
    "map --eval \"genUnique()\" --srctable addata#6c24 --fieldName c25 "
    "--dsttable addata#6c25;"
    "map --eval \"genUnique()\" --srctable addata#6c25 --fieldName c26 "
    "--dsttable addata#6c26;"
    "map --eval \"genUnique()\" --srctable addata#6c26 --fieldName c27 "
    "--dsttable addata#6c27;"
    "map --eval \"genUnique()\" --srctable addata#6c27 --fieldName c28 "
    "--dsttable addata#6c28;"
    "map --eval \"genUnique()\" --srctable addata#6c28 --fieldName c29 "
    "--dsttable addata#6c29;"
    "map --eval \"genUnique()\" --srctable addata#6c29 --fieldName c30 "
    "--dsttable addata#6c30;"
    "map --eval \"genUnique()\" --srctable addata#6c30 --fieldName c31 "
    "--dsttable addata#6c31;"
    "map --eval \"genUnique()\" --srctable addata#6c31 --fieldName c32 "
    "--dsttable addata#6c32;"
    "map --eval \"genUnique()\" --srctable addata#6c32 --fieldName c33 "
    "--dsttable addata#6c33;"
    "map --eval \"genUnique()\" --srctable addata#6c33 --fieldName c34 "
    "--dsttable addata#6c34;"
    "map --eval \"genUnique()\" --srctable addata#6c34 --fieldName c35 "
    "--dsttable addata#6c35;"
    "map --eval \"genUnique()\" --srctable addata#6c35 --fieldName c36 "
    "--dsttable addata#6c36;"
    "map --eval \"genUnique()\" --srctable addata#6c36 --fieldName c37 "
    "--dsttable addata#6c37;"
    "map --eval \"genUnique()\" --srctable addata#6c37 --fieldName c38 "
    "--dsttable addata#6c38;"
    "map --eval \"genUnique()\" --srctable addata#6c38 --fieldName c39 "
    "--dsttable addata#6c39;"
    "map --eval \"genUnique()\" --srctable addata#6c39 --fieldName c40 "
    "--dsttable addata#6c40;"
    "map --eval \"genUnique()\" --srctable addata#6c40 --fieldName c41 "
    "--dsttable addata#6c41;"
    "map --eval \"genUnique()\" --srctable addata#6c41 --fieldName c42 "
    "--dsttable addata#6c42;"
    "map --eval \"genUnique()\" --srctable addata#6c42 --fieldName c43 "
    "--dsttable addata#6c43;"
    "map --eval \"genUnique()\" --srctable addata#6c43 --fieldName c44 "
    "--dsttable addata#6c44;"
    "map --eval \"genUnique()\" --srctable addata#6c44 --fieldName c45 "
    "--dsttable addata#6c45;"
    "map --eval \"genUnique()\" --srctable addata#6c45 --fieldName c46 "
    "--dsttable addata#6c46;"
    "map --eval \"genUnique()\" --srctable addata#6c46 --fieldName c47 "
    "--dsttable addata#6c47;"
    "map --eval \"genUnique()\" --srctable addata#6c47 --fieldName c48 "
    "--dsttable addata#6c48;"
    "map --eval \"genUnique()\" --srctable addata#6c48 --fieldName c49 "
    "--dsttable addata#6c49;"
    "map --eval \"genUnique()\" --srctable addata#6c49 --fieldName c50 "
    "--dsttable addata#6c50;"
    "map --eval \"genUnique()\" --srctable addata#6c50 --fieldName c51 "
    "--dsttable addata#6c51;"
    "map --eval \"genUnique()\" --srctable addata#6c51 --fieldName c52 "
    "--dsttable addata#6c52;"
    "map --eval \"genUnique()\" --srctable addata#6c52 --fieldName c53 "
    "--dsttable addata#6c53;"
    "map --eval \"genUnique()\" --srctable addata#6c53 --fieldName c54 "
    "--dsttable addata#6c54;"
    "map --eval \"genUnique()\" --srctable addata#6c54 --fieldName c55 "
    "--dsttable addata#6c55;"
    "map --eval \"genUnique()\" --srctable addata#6c55 --fieldName c56 "
    "--dsttable addata#6c56;"
    "map --eval \"genUnique()\" --srctable addata#6c56 --fieldName c57 "
    "--dsttable addata#6c57;"
    "map --eval \"genUnique()\" --srctable addata#6c57 --fieldName c58 "
    "--dsttable addata#6c58;"
    "map --eval \"genUnique()\" --srctable addata#6c58 --fieldName c59 "
    "--dsttable addata#6c59;"
    "map --eval \"genUnique()\" --srctable addata#6c59 --fieldName c60 "
    "--dsttable addata#6c60;"
    "map --eval \"genUnique()\" --srctable addata#6c60 --fieldName c61 "
    "--dsttable addata#6c61;"
    "map --eval \"genUnique()\" --srctable addata#6c61 --fieldName c62 "
    "--dsttable addata#6c62;"
    "map --eval \"genUnique()\" --srctable addata#6c62 --fieldName c63 "
    "--dsttable addata#6c63;"
    "map --eval \"genUnique()\" --srctable addata#6c63 --fieldName c64 "
    "--dsttable addata#6c64;"
    "map --eval \"genUnique()\" --srctable addata#6c64 --fieldName c65 "
    "--dsttable addata#6c65;"
    "map --eval \"genUnique()\" --srctable addata#6c65 --fieldName c66 "
    "--dsttable addata#6c66;"
    "map --eval \"genUnique()\" --srctable addata#6c66 --fieldName c67 "
    "--dsttable addata#6c67;"
    "map --eval \"genUnique()\" --srctable addata#6c67 --fieldName c68 "
    "--dsttable addata#6c68;"
    "map --eval \"genUnique()\" --srctable addata#6c68 --fieldName c69 "
    "--dsttable addata#6c69;"
    "load --url nfs:///netstore/datasets/financial --format csv --size 0B  "
    "--name asdf1.financial12.%s --crlf --header "
    "--apply generator:gen10ColDataset;"
    "map --eval \"genUnique()\" --srctable addata#6c69 --fieldName c70 "
    "--dsttable addata#6c70;"
    "index --key xcalarRecordNum --dataset .XcalarDS.asdf1.financial12.%s "
    "--dsttable financial1#6c82 --prefix p;"
    "map --eval \"genUnique()\" --srctable addata#6c70 --fieldName c71 "
    "--dsttable addata#6c71;"
    "map --eval \"int(right9)\" --srctable financial1#6c82 "
    "--fieldName right9_integer --dsttable financial1#6c86;"
    "map --eval \"genUnique()\" --srctable addata#6c71 "
    "--fieldName c72 --dsttable addata#6c72;"
    "map --eval \"int(right8)\" --srctable financial1#6c86 "
    "--fieldName right8_integer --dsttable financial1#6c87;"
    "map --eval \"genUnique()\" --srctable addata#6c72 "
    "--fieldName c73 --dsttable addata#6c73;"
    "map --eval \"int(right7)\" --srctable financial1#6c87 "
    "--fieldName right7_integer --dsttable financial1#6c88;"
    "map --eval \"genUnique()\" --srctable addata#6c73 "
    "--fieldName c74 --dsttable addata#6c74;"
    "map --eval \"int(right6)\" --srctable financial1#6c88 "
    "--fieldName right6_integer --dsttable financial1#6c89;"
    "map --eval \"genUnique()\" --srctable addata#6c74 "
    "--fieldName c75 --dsttable addata#6c75;"
    "map --eval \"int(right5)\" --srctable financial1#6c89 "
    "--fieldName right5_integer --dsttable financial1#6c90;"
    "map --eval \"genUnique()\" --srctable addata#6c75 "
    "--fieldName c76 --dsttable addata#6c76;"
    "map --eval \"int(right4)\" --srctable financial1#6c90 "
    "--fieldName right4_integer --dsttable financial1#6c91;"
    "map --eval \"genUnique()\" --srctable addata#6c76 "
    "--fieldName c77 --dsttable addata#6c77;"
    "map --eval \"int(right3)\" --srctable financial1#6c91 "
    "--fieldName right3_integer --dsttable financial1#6c92;"
    "map --eval \"genUnique()\" --srctable addata#6c77 "
    "--fieldName c78 --dsttable addata#6c78;"
    "map --eval \"int(right2)\" --srctable financial1#6c92 "
    "--fieldName right2_integer --dsttable financial1#6c93;"
    "map --eval \"genUnique()\" --srctable addata#6c78 "
    "--fieldName c79 --dsttable addata#6c79;"
    "map --eval \"int(right1)\" --srctable financial1#6c93 "
    "--fieldName right1_integer --dsttable financial1#6c94;"
    "map --eval \"genUnique()\" --srctable addata#6c79 "
    "--fieldName c80 --dsttable addata#6c80;"
    "map --eval \"int(right0)\" --srctable financial1#6c94 "
    "--fieldName right0_integer --dsttable financial1#6c95;"
    "map --eval \"genUnique()\" --srctable addata#6c80 --fieldName c81 "
    "--dsttable addata#6c81;"
    "map --eval \"float(right0_integer)\" --srctable financial1#6c95 "
    "--fieldName right0_integer_float --dsttable financial1#6c99;"
    "map --eval \"sub(c0, 1524671)\" --srctable addata#6c81 "
    "--fieldName leftJoin --dsttable addata#6c84;"
    "map --eval \"float(leftJoin)\" --srctable addata#6c84 "
    "--fieldName leftJoin_float --dsttable addata#6c85;"
    "index --key right0_integer_float --srctable financial1#6c99 "
    "--dsttable financial1.index#6c101;"
    "index --key leftJoin_float --srctable addata#6c85 "
    "--dsttable addata.index#6c102;"
    "join --leftTable financial1.index#6c101 --rightTable addata.index#6c102 "
    "--joinType innerJoin  --joinTable finallyJoined#6c100;"
    "drop table *; drop constant *; drop dataset *;";

#endif  // _TESTQUERIES_H_