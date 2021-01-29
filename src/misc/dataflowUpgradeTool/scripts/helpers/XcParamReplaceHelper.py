# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

class PerParamInfo(object):
    def __init__(self, param, value):
        assert(len(param) > 0)
        assert(len(value) > 0)
        self.param = param
        self.value = value

class XcParamReplaceHelper(object):

    def __init__(self, verbose = False):
        self.verbose = verbose

        self.replacedParamsInfo = []

    def getThisParamInfo(self, param):
        for paramInfo in self.replacedParamsInfo:
            if (paramInfo.param == param):
                return (paramInfo)

        return (None)

    def getValueFromUser(self, question):
        while True:
            replacement = input(question)
            if (not replacement):
                continue
            if (not (len(replacement) > 0)):
                continue
            return (replacement)

    def replaceParameterInt(self, currentOp):
        if ("<" not in currentOp):
            assert(">" not in currentOp)
            return (currentOp)

        if self.verbose:
            print("==============================================================")

        subParams = currentOp.split("--")
        outputList = []

        for idx, currentSubParam in enumerate(subParams):
            currentLine = currentSubParam
            while ("<" in currentLine and ">" in currentLine):
                preParamSubString = currentLine.split("<", 1)[0]
                postParamSubString = currentLine.split(">", 1)[1]
                param = currentLine.split("<", 1)[1].split(">", 1)[0].strip()
                paramInfo = self.getThisParamInfo(param)
                replacementValue = None
                if (paramInfo is None):
                    question = "In '%s', please enter the value for '%s'\n"\
                        % (currentOp, param)
                    replacementValue = self.getValueFromUser(question)
                    # save this in case same param shows up again
                    perParamInfo = PerParamInfo(param, replacementValue)
                    self.replacedParamsInfo.append(perParamInfo)
                else:
                    if self.verbose:
                        print("Using %s from previously entered value for param: %s"
                                % (paramInfo.value, param))
                    replacementValue = paramInfo.value

                currentLine = preParamSubString + replacementValue +\
                    postParamSubString

            if (idx != 0):
                currentLine = "--" + currentLine

            outputList.append(currentLine)

        return ("".join(outputList))

    def replaceParameters(self, inputQuery):

        paramReplacedQuery = ""

        if ("<" not in inputQuery):
            assert(">" not in inputQuery)
            return (inputQuery)

        listOfOperations = inputQuery.strip().split(";")

        for operation in listOfOperations:

            paramReplacedOp = self.replaceParameterInt(operation)
            if (paramReplacedOp is None):
                return (None)

            if (len(paramReplacedOp) > 0):
                paramReplacedQuery += paramReplacedOp + ";"

        return (paramReplacedQuery)
