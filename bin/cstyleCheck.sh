#!/bin/bash

TMP1=`mktemp /tmp/XXXXXXcstyle1`
TMP2=`mktemp /tmp/XXXXXXcstyle2`
rm -f $TMP1 $TMP2

echo "1..1"

filesToIgnore="config.h LogMessages.h Status.cpp Subsys.h Status.h "\
"GenericTypesEnums_constants.h GenericTypesEnums_types.h "\
"LibApisCommon_types.h XcalarApiService.h Status_constants.h "\
"LibApisCommon_constants.h Status_types.h HelloService.h hello_types.h "\
"hello_constants.h DataFormatEnums.c Subsys.c OperatorsEnums_types.h "\
"OperatorsEnums_constants.h DataFormatEnums_types.h "\
"DataFormatEnums_constants.h LibApisEnums_constants.h LibApisEnums_types.h "\
"LibDsTest.c XcalarApiVersionSignature.cpp XcalarApiVersionSignature_types.h "\
"DataTargetEnums.cpp DataTargetEnums_types.h DataTargetEnums_constants.h "\
"DataTargetEnums_constants.cpp DataTargetEnums_types.cpp "\
"DataTargetTypes.c DataTargetTypes_types.h DataTargetTypes_constants.h "\
"DataTargetTypes_constants.cpp DataTargetTypes_types.cpp "\
"DagTypes_constants.h DagTypes_types.h DagTypes_constants.cpp DagTypes_types.cpp "\
"UdfTypes_types.h UdfTypes_constants.h UdfTypes_constants.cpp UdfTypes_types.cpp "\
"XcalarApiVersionSignature_constants.h LibApisEnums_constants.cpp "\
"XcalarApiService_server.skeleton.cpp GenericTypesEnums_types.cpp "\
"OperatorsEnums_types.cpp DataFormatEnums_constants.cpp "\
"DataFormatEnums_types.cpp XcalarApiService.cpp Status_constants.cpp "\
"Status_types.cpp  GenericTypesEnums_constants.cpp LibApisEnums_types.cpp "\
"LibApisCommon_types.cpp LibApisCommon_constants.cpp "\
"OperatorsEnums_constants.cpp XcalarApiVersionSignature_constants.cpp "\
"AggregateOpEnums_constants.cpp AggregateOpEnums_types.cpp "\
"LibApisConstants_constants.cpp JoinOpEnums_constants.cpp JoinOpEnums_types.h "\
"JoinOpEnums_types.cpp AggregateOpEnums_types.h LibApisConstants_constants.h "\
"JoinOpEnums_constants.h LibApisConstants_types.h "\
"AggregateOpEnums_constants.h LibApisConstants_types.cpp "\
"XcalarApiVersionSignature_types.cpp hello_types.cpp hello_constants.cpp "\
"HelloService_server.skeleton.cpp HelloService.cpp QueryStateEnums_types.h "\
"QueryStateEnums_constants.h QueryStateEnums_types.cpp "\
"QueryStateEnums_constants.cpp DagStateEnums_types.h "\
"DagStateEnums_constants.h DagStateEnums_types.cpp "\
"DagStateEnums_constants.cpp GetOpt.c GetOpt.h BitmapTypes.h Bitmap.cpp "\
"FunctionCategory_constants.h FunctionCategory_types.h FunctionCategory_constants.cpp "\
"FunctionCategory_types.cpp SourceTypeEnum_types.h "\
"SourceTypeEnum_constants.h SourceTypeEnum_types.cpp "\
"SourceTypeEnum_constants.cpp OrderingEnums_types.h "\
"OrderingEnums_constants.h OrderingEnums_types.cpp "\
"OrderingEnums_constants.cpp crc32c.c bitmapBenchmarks.cpp"\
"*.pb.* "\
"LibDsTest.cpp DataSetTestsCommon.cpp TupleTypes.h "\
"DataFormatTests.cpp LibQueryParserFuncTestConfig.h LibQmFuncTestConfig.cpp "\
"LibKvStoreFuncTestConfig.cpp LibQueryParserFuncTestConfig.cpp "\
"LibBcFuncTestConfig.cpp LibRuntimeFuncTestConfig.cpp LibMsgFuncTestConfig.cpp "\
"LibNsFuncTestConfig.cpp LibDagFuncTestConfig.cpp ChildFunFuncTestConfig.cpp "\
"LibXdbFuncTestConfig.cpp LibLogFuncTestConfig.cpp LibQueryEvalFuncTestConfig.cpp "\
"LibOperatorsFuncTestConfig.cpp LibDsFuncTestConfig.cpp LibDfFuncTestConfig.cpp "\
"LibSessionFuncTestConfig.cpp LibOptimizerFuncTestConfig.cpp "\
"DagDurable.cpp DagDurable.h DagDurableMinorVers.cpp "\
"KvStoreDurable.cpp KvStoreDurable.h KvStoreDurableMinorVers.cpp "\
"DataTargetDurable.cpp DataTargetDurable.h DataTargetDurableMinorVers.cpp "\
"SessionDurable.cpp SessionDurableMinorVers.cpp SessionDurable.h "\
"DurableVersions.h lmdb.h mdb.c midl.c midl.h"

EXTRA_ARGS=""
for file in $filesToIgnore
do
    EXTRA_ARGS+=" -a ! -name $file"
done

# really just want *.[ch] but fancy footwork needed to avoid autoconf's config.h
# if you change this you must also change $(top_srcdir)/make/baseCommon.am
cstyle.pl `find . -maxdepth 1 -xtype f -regex '.*\.[ch]' $EXTRA_ARGS && find . -maxdepth 1 -xtype f -regex '.*\.cpp' $EXTRA_ARGS` > $TMP2 2>&1
if [ "$?" != "0" ]
then
    echo "============================================================" 1>&2
    echo "*** CSTYLE CHECK FAILED for `pwd`/*.[ch]!" 1>&2
    cat $TMP2 1>&2
    echo "============================================================" 1>&2
    touch $TMP1
fi

retval=0
if [ -e "$TMP1" ]
then
    retval=1
fi

rm -f $TMP1 $TMP2

if [ "$retval" = "0" ]; then
    echo "ok 1"
else
    echo "not ok 1"
fi

exit $retval
