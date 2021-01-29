#!/bin/bash

EXEMPT_FILES="config.h \
              DataFormatEnums_constants.h DataFormatEnums_types.h \
              SourceTypeEnum_constants.h SourceTypeEnum_types.h \
              DataTargetEnums_constants.h DataTargetEnums_types.h \
              DagRefTypeEnums_constants.h DagRefTypeEnums_types.h \
              DataTargetTypes_constants.h DataTargetTypes_types.h \
              DagTypes_constants.h DagTypes_types.h \
              UdfTypes_constants.h UdfTypes_types.h \
              hello_constants.h hello_types.h \
              GenericTypesEnums_constants.h GenericTypesEnums_types.h \
              LibApisCommon_constants.h LibApisCommon_types.h \
              LibApisEnums_constants.h LibApisEnums_types.h \
              OperatorsEnums_constants.h OperatorsEnums_types.h \
              Status_constants.h Status_constants.h \
              Status_types.h Status_types.h \
              XcalarApiVersionSignature_constants.h \
              XcalarApiVersionSignature_types.h \
              QueryStateEnums_types.h QueryStateEnums_constants.h \
              DagStateEnums_types.h DagStateEnums_constants.h \
              OrderingEnums_types.h OrderingEnums_constants.h \
              AggregateOpEnums_constants.h AggregateOpEnums_types.h \
              JoinOpEnums_constants.h JoinOpEnums_types.h \
              UnionOpEnums_constants.h UnionOpEnums_types.h \
              LibApisConstants_constants.h LibApisConstants_types.h \
              FunctionCategory_constants.h FunctionCategory_types.h \
              connection.h logMsg.h \
              msg.h refCounted.h \
              fastbinary.c feature_tests.c \
              QueryParserEnums.h \
              SIZEOF_DOUBLE.c SIZEOF_FD_SET.c SIZEOF_FLOAT.c SIZEOF_INT.c \
              SIZEOF_LONG_DOUBLE.c SIZEOF_LONG_INT.c SIZEOF_STRUCT_IOVEC.c \
              SIZEOF_STRUCT_SOCKADDR_IN.c SIZEOF_VA_LIST.c \
              ChildFunFuncTestConfig.h DagRefTypeEnums.h DagStateEnums.h \
              DataFormatEnums.h DataTargetEnums.h FunctionCategory.h \
              JoinOpEnums.h UnionOpEnums.h JsonGenEnums.h LibApisConstants.h LibApisEnums.h \
              LibAppFuncTestConfig.h LibBcFuncTestConfig.h LibDemystifyFuncTestConfig.h \
              LibDfFuncTestConfig.h LibDsFuncTestConfig.h LibKvStoreFuncTestConfig.h \
              LibLogFuncTestConfig.h LibMsgFuncTestConfig.h LibNs2FuncTestConfig.h \
              LibOperatorsFuncTestConfig.h LibOptimizerFuncTestConfig.h \
              LibQmFuncTestConfig.h LibQueryEvalFuncTestConfig.h LibQueryParserFuncTestConfig.h \
              LibRuntimeFuncTestConfig.h LibSessionFuncTestConfig.h \
              MigrationFuncTestConfig.h OrderingEnums.h QueryStateEnums.h SourceTypeEnums.h \
              Status.h Subsys.h UdfTypeEnums.h XcalarApiVersionSignature.h \
              LibDagFuncTestConfig.h LibXdbFuncTestConfig.h SourceTypeEnum.h"
# XXX temporarily exempt monitor from name checks

failhdr()
{
    if [ ! -e "$TMP2" ]
    then
        echo "============================================================" 1>&2
        echo "*** NAME CHECK FAILED!" 1>&2
    fi
}

echo "1..1"

TMP1=`mktemp /tmp/XXXXXXnamecheck1`
TMP2=`mktemp /tmp/XXXXXXnamecheck2`
TMP3=`mktemp /tmp/XXXXXXnamecheck3`
rm -f $TMP1 $TMP2 $TMP3

# Exempt 3rd party and virtual environment python packages
for filn in `find . -type f -name \*.[ch] | grep -v 3rd | grep -v site-packages`
do
    filn=`basename $filn`
    filnCap=`echo $filn | tr "[:lower:]" "[:upper:]"`
    echo "$filnCap:$filn" >> $TMP1
done

lastFiln=""
lastFilnCap=""
for filn in `sort $TMP1`
do
    filnCap=`echo $filn | cut -f1 -d':'`
    filn=`echo $filn | cut -f2 -d':'`
    echo $EXEMPT_FILES | grep -q $filn
    if [ "$?" = "0" ]
    then
        continue
    fi
    echo $filn | grep -q ".pb.h"
    if [ "$?" = "0" ]
    then
        continue
    fi

    if [ "$lastFilnCap" = "$filnCap" ]
    then
	failhdr
	echo "*** Duplicate case-insensitive filename: $filn && $lastFiln" 1>&2
	touch $TMP2
    fi
    lastFiln=$filn
    lastFilnCap=$filnCap

    echo $filn | egrep -q "^[A-Z][A-Za-z0-9]*\."
    if [ "$?" != "0" ]
    then
	failhdr
	echo "*** Filename not camelcase: $filn" 1>&2
	touch $TMP2
    fi
done

retval=0
if [ -e "$TMP2" ]
then
    echo "*** Please pick a unique case-insensitive name in CamelCase when " 1>&2
    echo "*** adding source files" 1>&2
    echo "============================================================" 1>&2
    retval=1
fi

rm -f $TMP1 $TMP2 $TMP3

if [ "$retval" = "0" ]; then
    echo "ok 1"
else
    echo "not ok 1"
fi

exit $retval
