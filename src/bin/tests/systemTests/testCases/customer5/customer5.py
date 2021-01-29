import sys

testCaseSpecifications = {
    'Enabled':
        False,
    'Name':
        'Customer5 Test',
    'Description':
        'Converted most of the customer5 POC into a list of cli commands',
    'ExpectedRunTime':
        100000,    # In seconds for 1 run
    'NumRuns':
        1,    # Number of times a given user will invoke main()
    'SingleThreaded':
        False,
    'ExclusiveSession':
        False,
}

# If NumRuns is set to 1, feel free to leave prepQuery blank.
# This might be useful if you just want to copy and paste a known bug into mainQuery
# prepQuery is only ever invoked once per user
prepQuery = """
load --url "nfs:///netstore/datasets/Customer5/gb_exp_100K.csv" --format csv --size 0B --name "customer5.gbexp100K" --fielddelim , --crlf --header;load --url "nfs:///netstore/datasets/Customer5/ipdr_exp_500K.csv" --format csv --size 0B --name "customer5.ipdrexp500K" --fielddelim , --crlf --header;
"""

# This gets invoked as many times as NumRuns specified in testCaseSpecifications
mainQuery = """
index --key "xcalarRecordNum" --dataset ".XcalarDS.customer5.gbexp100K" --dsttable "gbexp100K#jh0";
index --key "xcalarRecordNum" --dataset ".XcalarDS.customer5.ipdrexp500K" --dsttable "ipdrexp500K#jh1";
map --eval "int(STB_CAPABLE)" --srctable "gbexp100K#jh0" --fieldName "STB_CAPABLE_integer" --dsttable "gbexp100K#jh2";
map --eval "int(NUM_HOURS)" --srctable "gbexp100K#jh2" --fieldName "NUM_HOURS_integer" --dsttable "gbexp100K#jh3";
map --eval "float(KBPS_UP)" --srctable "gbexp100K#jh3" --fieldName "KBPS_UP_float" --dsttable "gbexp100K#jh4";
map --eval "float(KBPS_DOWN)" --srctable "gbexp100K#jh4" --fieldName "KBPS_DOWN_float" --dsttable "gbexp100K#jh5";
map --eval "int(MAC)" --srctable "gbexp100K#jh5" --fieldName "MAC_integer" --dsttable "gbexp100K#jh6";
map --eval "add(KBPS_DOWN_float, KBPS_UP_float)" --srctable "gbexp100K#jh6" --fieldName "KBPS_DOWN_float_add6" --dsttable "gbexp100K#jh7";
index --key "KBPS_DOWN_float_add6" --srctable "gbexp100K#jh7" --dsttable "gbexp100K#jh8" --sorted;
aggregate --srctable "gbexp100K#jh7" --dsttable "gbexp100K-aggregate#jh9", --eval "min(KBPS_DOWN_float_add6)";
aggregate --srctable "gbexp100K#jh7" --dsttable "gbexp100K-aggregate#jh10", --eval "avg(KBPS_DOWN_float_add6)";
aggregate --srctable "gbexp100K#jh7" --dsttable "gbexp100K-aggregate#jh11", --eval "max(KBPS_DOWN_float_add6)";
aggregate --srctable "gbexp100K#jh7" --dsttable "gbexp100K-aggregate#jh12", --eval "count(KBPS_DOWN_float_add6)";
aggregate --srctable "gbexp100K#jh7" --dsttable "gbexp100K-aggregate#jh13", --eval "sum(KBPS_DOWN_float_add6)";
index --key "KBPS_DOWN_float_add6" --srctable "gbexp100K#jh7" --dsttable "gbexp100K.indexParent65426#jh14";
groupBy --srctable "gbexp100K.indexParent65426#jh14" --eval "count(KBPS_DOWN_float_add6)" --fieldName statsGroupBy --dsttable "gbexp100K.profile.GB71151#jh15" --nosample;
index --key "KBPS_DOWN_float_add6" --srctable "gbexp100K.profile.GB71151#jh15" --dsttable "gbexp100K.profile.final35862#jh16" --sorted;
aggregate --srctable "gbexp100K.profile.GB71151#jh15" --dsttable "gbexp100K.profile.final35862-aggregate#jh17", --eval "max(statsGroupBy)";
aggregate --srctable "gbexp100K.profile.GB71151#jh15" --dsttable "gbexp100K.profile.final35862-aggregate#jh18", --eval "sum(statsGroupBy)";
drop table gbexp100K.indexParent65426#jh14;
map --eval "mult(KBPS_DOWN_float_add6, 3600)" --srctable "gbexp100K#jh7" --fieldName "KBPS_DOWN_float_add6_mult8" --dsttable "gbexp100K#jh19";
index --key "KBPS_DOWN_float_add6_mult8" --srctable "gbexp100K#jh19" --dsttable "gbexp100K#jh20" --sorted;
aggregate --srctable "gbexp100K#jh19" --dsttable "gbexp100K-aggregate#jh21", --eval "min(KBPS_DOWN_float_add6_mult8)";
aggregate --srctable "gbexp100K#jh19" --dsttable "gbexp100K-aggregate#jh22", --eval "avg(KBPS_DOWN_float_add6_mult8)";
aggregate --srctable "gbexp100K#jh19" --dsttable "gbexp100K-aggregate#jh23", --eval "max(KBPS_DOWN_float_add6_mult8)";
aggregate --srctable "gbexp100K#jh19" --dsttable "gbexp100K-aggregate#jh24", --eval "count(KBPS_DOWN_float_add6_mult8)";
aggregate --srctable "gbexp100K#jh19" --dsttable "gbexp100K-aggregate#jh25", --eval "sum(KBPS_DOWN_float_add6_mult8)";
index --key "KBPS_DOWN_float_add6_mult8" --srctable "gbexp100K#jh19" --dsttable "gbexp100K.indexParent25957#jh26";
groupBy --srctable "gbexp100K.indexParent25957#jh26" --eval "count(KBPS_DOWN_float_add6_mult8)" --fieldName statsGroupBy --dsttable "gbexp100K.profile.GB29778#jh27" --nosample;
index --key "KBPS_DOWN_float_add6_mult8" --srctable "gbexp100K.profile.GB29778#jh27" --dsttable "gbexp100K.profile.final25981#jh28" --sorted;
aggregate --srctable "gbexp100K.profile.GB29778#jh27" --dsttable "gbexp100K.profile.final25981-aggregate#jh29", --eval "max(statsGroupBy)";
aggregate --srctable "gbexp100K.profile.GB29778#jh27" --dsttable "gbexp100K.profile.final25981-aggregate#jh30", --eval "sum(statsGroupBy)";
drop table gbexp100K.indexParent25957#jh26;
map --eval "div(KBPS_DOWN_float_add6_mult8, 1048576)" --srctable "gbexp100K#jh19" --fieldName "KBPS_DOWN_float_add6_mult8_div20" --dsttable "gbexp100K#jh31";
aggregate --srctable "gbexp100K#jh31" --dsttable "gbexp100K-aggregate#jh32", --eval "min(KBPS_DOWN_float_add6_mult8_div20)";
aggregate --srctable "gbexp100K#jh31" --dsttable "gbexp100K-aggregate#jh34", --eval "max(KBPS_DOWN_float_add6_mult8_div20)";
aggregate --srctable "gbexp100K#jh31" --dsttable "gbexp100K-aggregate#jh33", --eval "avg(KBPS_DOWN_float_add6_mult8_div20)";
aggregate --srctable "gbexp100K#jh31" --dsttable "gbexp100K-aggregate#jh35", --eval "count(KBPS_DOWN_float_add6_mult8_div20)";
aggregate --srctable "gbexp100K#jh31" --dsttable "gbexp100K-aggregate#jh36", --eval "sum(KBPS_DOWN_float_add6_mult8_div20)";
index --key "KBPS_DOWN_float_add6_mult8_div20" --srctable "gbexp100K#jh31" --dsttable "gbexp100K.index#jh37";
aggregate --srctable "gbexp100K.index#jh37" --dsttable "gbexp100K.index-aggregate#jh38", --eval "count(KBPS_DOWN_float_add6_mult8_div20)";
groupBy --srctable "gbexp100K.index#jh37" --eval "count(KBPS_DOWN_float_add6_mult8_div20)" --fieldName statsGroupBy --dsttable "gbexp100K.profile.GB53763#jh39" --nosample;
index --key "KBPS_DOWN_float_add6_mult8_div20" --srctable "gbexp100K.profile.GB53763#jh39" --dsttable "gbexp100K.profile.final66173#jh40" --sorted;
aggregate --srctable "gbexp100K.profile.GB53763#jh39" --dsttable "gbexp100K.profile.final66173-aggregate#jh41", --eval "max(statsGroupBy)";
aggregate --srctable "gbexp100K.profile.GB53763#jh39" --dsttable "gbexp100K.profile.final66173-aggregate#jh42", --eval "sum(statsGroupBy)";
drop table gbexp100K.index#jh37;
map --eval "mult(floor(div(KBPS_DOWN_float_add6_mult8_div20, 50)), 50)" --srctable "gbexp100K.profile.GB53763#jh39" --fieldName "bucketMap4183" --dsttable "gbexp100K.profile.final66173.bucket#jh43";
index --key "bucketMap4183" --srctable "gbexp100K.profile.final66173.bucket#jh43" --dsttable "gbexp100K.profile.final66173.bucket.index47593#jh44";
groupBy --srctable "gbexp100K.profile.final66173.bucket.index47593#jh44" --eval "sum(statsGroupBy)" --fieldName bucketGroupBy --dsttable "gbexp100K.profile.final66173.bucket.groupby99942#jh45" --nosample;
index --key "bucketMap4183" --srctable "gbexp100K.profile.final66173.bucket.groupby99942#jh45" --dsttable "gbexp100K.profile.final66173.bucket.final85342#jh46" --sorted;
aggregate --srctable "gbexp100K.profile.final66173.bucket.groupby99942#jh45" --dsttable "gbexp100K.profile.final66173.bucket.final85342-aggregate#jh47", --eval "max(bucketGroupBy)";
aggregate --srctable "gbexp100K.profile.final66173.bucket.groupby99942#jh45" --dsttable "gbexp100K.profile.final66173.bucket.final85342-aggregate#jh48", --eval "sum(bucketGroupBy)";
drop table gbexp100K.profile.final66173.bucket#jh43;
drop table gbexp100K.profile.final66173.bucket.index47593#jh44;
map --eval "default:convertFormats(MONTH_FROM, \\\"%m/%d/%y\\\", \\\"%y%m\\\")" --srctable "gbexp100K#jh31" --fieldName "MONTH_FROM_udf31" --dsttable "gbexp100K#jh52";
index --key "MONTH_FROM_udf31" --srctable "gbexp100K#jh52" --dsttable "gbexp100K.index#jh53";
groupBy --srctable "gbexp100K.index#jh53" --eval "sum(KBPS_DOWN_float_add6_mult8_div20)" --fieldName MONTH_FROM_udf31_sum52 --dsttable "gbexp100K-GB#jh54" --nosample;
index --key "MAC_integer" --srctable "gbexp100K#jh52" --dsttable "gbexp100K.index#jh55";
groupBy --srctable "gbexp100K.index#jh55" --eval "sum(KBPS_DOWN_float_add6_mult8_div20)" --fieldName MAC_integer_sum52 --dsttable "gbexp100K-GB#jh56" --nosample;
index --key "ServiceClassName" --srctable "ipdrexp500K#jh1" --dsttable ".tempIndex.ipdrexp500K#jh57";
groupBy --srctable ".tempIndex.ipdrexp500K#jh57" --eval "count(ServiceClassName)" --fieldName randCol60131 --dsttable ".tempGB.ipdrexp500K#jh58" --nosample;
index --key "randCol60131" --srctable ".tempGB.ipdrexp500K#jh58" --dsttable ".tempGB-Sort.ipdrexp500K#jh59" --sorted=desc;
filter --srctable ipdrexp500K#jh1 --eval "eq(ServiceClassName, \\\"'sc_ds_pub_wifi'\\\")" --dsttable "ipdrexp500K-HP1#jh60";
filter --srctable ipdrexp500K#jh1 --eval "eq(ServiceClassName, \\\"'sc_us_pub_stb'\\\")" --dsttable "ipdrexp500K-HP2#jh61";
filter --srctable ipdrexp500K#jh1 --eval "eq(ServiceClassName, \\\"'sc_us_pub_wifi'\\\")" --dsttable "ipdrexp500K-HP3#jh62";
filter --srctable ipdrexp500K#jh1 --eval "eq(ServiceClassName, \\\"'sc_us_pub_voip'\\\")" --dsttable "ipdrexp500K-HP4#jh63";
map --eval "cut(CmLastRegTime, 1, \\\"-\\\")" --srctable "ipdrexp500K-HP1#jh60" --fieldName "CmLastRegTime-split-1" --dsttable "ipdrexp500K-HP1#jh64";
map --eval "cut(CmLastRegTime, 2, \\\"-\\\")" --srctable "ipdrexp500K-HP1#jh64" --fieldName "CmLastRegTime-split-2" --dsttable "ipdrexp500K-HP1#jh65";
map --eval "default:splitWithDelim(CmLastRegTime, 2, \\\"-\\\")" --srctable "ipdrexp500K-HP1#jh65" --fieldName "CmLastRegTime-split-rest" --dsttable "ipdrexp500K-HP1#jh66";
map --eval "substring(CmLastRegTime-split-1, 2, 0)" --srctable "ipdrexp500K-HP1#jh66" --fieldName "CmLastRegTime-split-1_substring66" --dsttable "ipdrexp500K-HP1#jh67";
map --eval "concat(CmLastRegTime-split-1_substring66, CmLastRegTime-split-2)" --srctable "ipdrexp500K-HP1#jh67" --fieldName "CmLastRegTime-split-1_substring66_concat67" --dsttable "ipdrexp500K-HP1#jh68";
index --key "CmLastRegTime-split-1_substring66_concat67" --srctable "ipdrexp500K-HP1#jh68" --dsttable "ipdrexp500K-HP1.index#jh70";
join --leftTable "ipdrexp500K-HP1.index#jh70" --rightTable "gbexp100K-GB#jh54" --joinType innerJoin --joinTable "joinMonth#jh69";
map --eval "substring(CmMacAddr, 8, 0)" --srctable "ipdrexp500K-HP1.index#jh70" --fieldName "CmMacAddr_substring70" --dsttable "ipdrexp500K-HP1.index#jh71";
map --eval "replace(CmMacAddr_substring70, \\\"-\\\", \\\"''\\\")" --srctable "ipdrexp500K-HP1.index#jh71" --fieldName "CmMacAddr_substring70_replace71" --dsttable "ipdrexp500K-HP1.index#jh72";
map --eval "replace(CmMacAddr_substring70_replace71, \\\"''\\\", \\\":\\\")" --srctable "ipdrexp500K-HP1.index#jh72" --fieldName "CmMacAddr_substring70_replace71_replace72" --dsttable "ipdrexp500K-HP1.index#jh73";
map --eval "countChar(CmMacAddr_substring70_replace71_replace72, \\\":\\\")" --srctable "ipdrexp500K-HP1.index#jh73" --fieldName "mappedCol80015" --dsttable ".tempMap.ipdrexp500K-HP1.index#jh75";
aggregate --srctable ".tempMap.ipdrexp500K-HP1.index#jh75" --dsttable ".tempMap.ipdrexp500K-HP1.index-aggregate#jh76", --eval "maxInteger(mappedCol80015)";
map --eval "cut(CmMacAddr_substring70_replace71_replace72, 1, \\\":\\\")" --srctable "ipdrexp500K-HP1.index#jh73" --fieldName "CmMacAddr_substring70_replace71_replace72-split-1" --dsttable "ipdrexp500K-HP1.index#jh77";
map --eval "cut(CmMacAddr_substring70_replace71_replace72, 2, \\\":\\\")" --srctable "ipdrexp500K-HP1.index#jh77" --fieldName "CmMacAddr_substring70_replace71_replace72-split-2" --dsttable "ipdrexp500K-HP1.index#jh78";
map --eval "cut(CmMacAddr_substring70_replace71_replace72, 3, \\\":\\\")" --srctable "ipdrexp500K-HP1.index#jh78" --fieldName "CmMacAddr_substring70_replace71_replace72-split-3" --dsttable "ipdrexp500K-HP1.index#jh79";
map --eval "cut(CmMacAddr_substring70_replace71_replace72, 4, \\\":\\\")" --srctable "ipdrexp500K-HP1.index#jh79" --fieldName "CmMacAddr_substring70_replace71_replace72-split-4" --dsttable "ipdrexp500K-HP1.index#jh80";
map --eval "cut(CmMacAddr_substring70_replace71_replace72, 5, \\\":\\\")" --srctable "ipdrexp500K-HP1.index#jh80" --fieldName "CmMacAddr_substring70_replace71_replace72-split-5" --dsttable "ipdrexp500K-HP1.index#jh81";
map --eval "cut(CmMacAddr_substring70_replace71_replace72, 6, \\\":\\\")" --srctable "ipdrexp500K-HP1.index#jh81" --fieldName "CmMacAddr_substring70_replace71_replace72-split-6" --dsttable "ipdrexp500K-HP1.index#jh82";
map --eval "cut(CmMacAddr_substring70_replace71_replace72, 7, \\\":\\\")" --srctable "ipdrexp500K-HP1.index#jh82" --fieldName "CmMacAddr_substring70_replace71_replace72-split-7" --dsttable "ipdrexp500K-HP1.index#jh83";
map --eval "concat(CmMacAddr_substring70_replace71_replace72-split-1, CmMacAddr_substring70_replace71_replace72-split-2)" --srctable "ipdrexp500K-HP1.index#jh83" --fieldName "CmMacAddr_substring70_replace71_replace72-split-1_concat83" --dsttable "ipdrexp500K-HP1.index#jh84";
map --eval "concat(CmMacAddr_substring70_replace71_replace72-split-1_concat83, CmMacAddr_substring70_replace71_replace72-split-3)" --srctable "ipdrexp500K-HP1.index#jh84" --fieldName "CmMacAddr_substring70_replace71_replace72-split-1_concat83_concat84" --dsttable "ipdrexp500K-HP1.index#jh85";
map --eval "concat(CmMacAddr_substring70_replace71_replace72-split-1_concat83_concat84, CmMacAddr_substring70_replace71_replace72-split-4)" --srctable "ipdrexp500K-HP1.index#jh85" --fieldName "CmMacAddr_substring70_replace71_replace72-split-1_concat83_concat84_concat85" --dsttable "ipdrexp500K-HP1.index#jh86";
map --eval "concat(CmMacAddr_substring70_replace71_replace72-split-1_concat83_concat84_concat85, CmMacAddr_substring70_replace71_replace72-split-5)" --srctable "ipdrexp500K-HP1.index#jh86" --fieldName "CmMacAddr_substring70_replace71_replace72-split-1_concat83_concat84_concat85_concat86" --dsttable "ipdrexp500K-HP1.index#jh87";
map --eval "concat(CmMacAddr_substring70_replace71_replace72-split-1_concat83_concat84_concat85_concat86, CmMacAddr_substring70_replace71_replace72-split-5)" --srctable "ipdrexp500K-HP1.index#jh87" --fieldName "CmMacAddr_substring70_replace71_replace72-split-1_concat83_concat84_concat85_concat86_concat87" --dsttable "ipdrexp500K-HP1.index#jh88";
map --eval "concat(CmMacAddr_substring70_replace71_replace72-split-1_concat83_concat84_concat85_concat86, CmMacAddr_substring70_replace71_replace72-split-6)" --srctable "ipdrexp500K-HP1.index#jh88" --fieldName "CmMacAddr_substring70_replace71_replace72-split-1_concat83_concat84_concat85_concat86_concat88" --dsttable "ipdrexp500K-HP1.index#jh89";
map --eval "int(CmMacAddr_substring70_replace71_replace72-split-1_concat83_concat84_concat85_concat86_concat88)" --srctable "ipdrexp500K-HP1.index#jh89" --fieldName "CmMacAddr_substring70_replace71_replace72-split-1_concat83_concat84_concat85_concat86_concat88_integer" --dsttable "ipdrexp500K-HP1.index#jh90";
index --key "CmMacAddr_substring70_replace71_replace72-split-1_concat83_concat84_concat85_concat86_concat88_integer" --srctable "ipdrexp500K-HP1.index#jh90" --dsttable "ipdrexp500K-HP1.index.index#jh92";
join --leftTable "ipdrexp500K-HP1.index.index#jh92" --rightTable "gbexp100K-GB#jh56" --joinType innerJoin --joinTable "joinMac#jh91";
"""

cleanupQuery = """
drop table \"*\";
drop constant \"*\";
drop dataset \"*\";
"""


def formatQuery(logging, TestCaseStatus, testSpec=None):
    return TestCaseStatus.Pass


def globalPrepare(logging, xcInstance, TestCaseStatus, testSpec=None):
    return TestCaseStatus.Pass


def prepare(logging, xcInstance, TestCaseStatus, testSpec=None):
    (queryName, queryOutput) = xcInstance.runQuery(prepQuery)
    if queryName is None:
        return TestCaseStatus.Fail

    xcInstance.waitForQuery(queryName)
    return TestCaseStatus.Pass


def main(logging, runNum, xcInstance, TestCaseStatus, testSpec=None):
    (queryName, queryOutput) = xcInstance.runQuery(mainQuery)
    if queryName is None:
        return TestCaseStatus.Fail

    xcInstance.waitForQuery(queryName)
    return TestCaseStatus.Pass


def verify(logging, runNum, xcInstance, TestCaseStatus, testSpec=None):
    numRowsMac = xcInstance.getRowsInTable("joinMac#jh91")
    numRowsMonth = xcInstance.getRowsInTable("joinMonth#jh69")

    if numRowsMac != 119955:
        logging.error("Mac rows %d != 119955" % numRowsMac)
        return TestCaseStatus.Fail

    if numRowsMonth != 119973:
        logging.error("Month rows %d != 119973" % numRowsMonth)
        return TestCaseStatus.Fail
    else:
        return TestCaseStatus.Pass


def cleanup(logging, xcInstance, finalStatus, TestCaseStatus):
    xcInstance.runQuery(cleanupQuery)


def globalCleanUp(logging, xcInstance, TestCaseStatus):
    # Delist datasets created by this test
    xcInstance.xcalarApi.deleteDatasets("*customer5*", xcInstance.username,
                                        xcInstance.userIdUnique)

    return TestCaseStatus.Pass


if __name__ == "__main__":
    print("This test case should not be executed as a standalone")
    sys.exit(0)
