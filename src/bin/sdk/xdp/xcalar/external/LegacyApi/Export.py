# Copyright 2016-2017 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

# flake8: noqa
# This disables flake8 static analysis for this file. It is going away with
# Export 2.0 anyway.

from .WorkItem import WorkItemExport
from xcalar.compute.coretypes.OrderingEnums.constants import *
from xcalar.compute.coretypes.JoinOpEnums.ttypes import *
from xcalar.compute.coretypes.DataFormatEnums.ttypes import *
from xcalar.compute.coretypes.DataFormatEnums.constants import *
from xcalar.compute.coretypes.DataTargetTypes.ttypes import *
from xcalar.compute.coretypes.DataTargetEnums.ttypes import *
from xcalar.compute.coretypes.DataTargetEnums.constants import *


def getFormatType(s):
    return list(DfFormatTypeTStr.values()).index(s)


def getCreateRuleType(s):
    return list(ExExportCreateRuleTStr.values()).index(s)


def getHeaderType(s):
    return list(ExSFHeaderTypeTStr.values()).index(s)


def getSplitType(s):
    return list(ExSFFileSplitTypeTStr.values()).index(s)


def getTargetType(s):
    return list(ExTargetTypeTStr.values()).index(s)


class Export(object):
    def __init__(self, xcalarApi):
        self.xcalarApi = xcalarApi

    def sf(self, tableName, fmtStr, formatArgs, columns, args):
        targType = getTargetType("file")
        target = ExExportTargetHdrT(type=targType, name=args["targetName"])

        splitRule = ExSFFileSplitRuleT(
            type=getSplitType(args["splitRule"]),
            spec=ExSFFileSplitSpecificT(maxSize=args["maxSize"]))
        specInput = ExInitExportSpecificInputT(
            sfInput=ExInitExportSFInputT(
                fileName=args["fileName"],
                format=getFormatType(fmtStr),
                splitRule=splitRule,
                headerType=getHeaderType(args["headerType"]),
                formatArgs=formatArgs))

        createRule = getCreateRuleType(args["createType"])

        # Set up the columns into xcalar columns
        xcalarColumns = []
        for cn in columns:
            fieldName = None
            aliasName = None
            if isinstance(cn, str):
                fieldName = cn
                aliasName = cn
            else:
                fieldName = cn[0]
                aliasName = cn[1]
            xcalarColumns.append(
                ExColumnNameT(name=fieldName, headerAlias=aliasName))

        workItem = WorkItemExport(tableName, target, specInput, createRule,
                                  args["makeSorted"], xcalarColumns)
        return self.xcalarApi.execute(workItem)

    def csv(self, tableName, columns, **kwargs):
        """Exports the 'columns' of the table
        args allows for customization of the rules. See defaultArgs for options.
        tableName - the name of the table to be exported
        columns  - an array of either strings or tuples where the first element
        is the column name and the second is the alias.
        Ex. ["firstCol", ("SecondCol", "mySpecailNameForSecondCol"), "thirdcol"]
        """
        defaultArgs = {
            "targetName": "Default",
            "fileName": tableName + ".csv",
            "splitRule": "none",
            "maxSize": 1 * 2**20,    # 1MB
            "headerType": "separate",
            "createType": "deleteAndReplace",
            "makeSorted": True,
        # CSV specific args
            "fieldDelim": '\t',
            "recordDelim": '\n',
            "quoteDelim": '"',
            "isUdf": False
        }

        args = {}
        # set non-defaults
        for (prop, default) in defaultArgs.items():
            args[prop] = kwargs.get(prop, default)

        csvArgs = ExInitExportCSVArgsT(
            fieldDelim=args["fieldDelim"],
            recordDelim=args["recordDelim"],
            quoteDelim=args["quoteDelim"])
        formatArgs = ExInitExportFormatSpecificArgsT(csv=csvArgs)

        if (args["isUdf"]):
            self.udf(tableName, "csv", formatArgs, columns, args)
        else:
            self.sf(tableName, "csv", formatArgs, columns, args)

    def sql(self, tableName, columns, **kwargs):
        """Exports the 'columns' of the table
        args allows for customization of the rules. See defaultArgs for options.
        tableName - the name of the table to be exported
        columns  - an array of either strings or tuples where the first element
        is the column name and the second is the alias.
        Ex. ["firstCol", ("SecondCol", "mySpecailNameForSecondCol"), "thirdcol"]
        """
        defaultArgs = {
            "targetName": "Default",
            "fileName": tableName + ".sql",
            "splitRule": "none",
            "maxSize": 1 * 2**20,    # 1MB
            "headerType": "separate",
            "createType": "deleteAndReplace",
            "makeSorted": False,
        # SQL specific args
            "tableName": tableName,
            "dropTable": True,
            "createTable": True,
            "isUdf": False
        }

        args = {}
        # set non-defaults
        for (prop, default) in defaultArgs.items():
            args[prop] = kwargs.get(prop, default)

        sqlArgs = ExInitExportSQLArgsT(
            tableName=args["tableName"],
            dropTable=args["dropTable"],
            createTable=args["createTable"])
        formatArgs = ExInitExportFormatSpecificArgsT(sql=sqlArgs)

        if (args["isUdf"]):
            self.udf(tableName, "sql", formatArgs, columns, args)
        else:
            self.sf(tableName, "sql", formatArgs, columns, args)

    def udf(self, tableName, fmtStr, formatArgs, columns, args):
        targType = getTargetType("udf")
        target = ExExportTargetHdrT(type=targType, name=args["targetName"])

        specInput = ExInitExportSpecificInputT(
            udfInput=ExInitExportUDFInputT(
                fileName=args["fileName"],
                format=getFormatType(fmtStr),
                headerType=getHeaderType(args["headerType"]),
                formatArgs=formatArgs))

        createRule = getCreateRuleType(args["createType"])

        # Set up the columns into xcalar columns
        xcalarColumns = []
        for cn in columns:
            fieldName = None
            aliasName = None
            if isinstance(cn, str):
                fieldName = cn
                aliasName = cn
            else:
                fieldName = cn[0]
                aliasName = cn[1]
            xcalarColumns.append(
                ExColumnNameT(name=fieldName, headerAlias=aliasName))

        workItem = WorkItemExport(tableName, target, specInput, createRule,
                                  args["makeSorted"], xcalarColumns)
        return self.xcalarApi.execute(workItem)
