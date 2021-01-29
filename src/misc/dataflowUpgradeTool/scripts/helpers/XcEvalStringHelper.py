# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import json
from json import JSONEncoder

import pyparsing as pp
import sys

def _default(self, obj):
    return getattr(obj.__class__, "to_json", _default.default)(obj)

_default.default = JSONEncoder().default
JSONEncoder.default = _default

class XcEvalStringHelper(object):
    def __init__(self, verbose = False, listOfUdfNames = []):
        self.verbose = verbose

        self.listOfUdfNames = listOfUdfNames

        # if there is an eval string which contains <UdfName>:<Function>
        # libqp will complain(saying function not registered)
        # since the parse functions are stateful
        # working around this by inserting a valid eval string
        # which we will replace after libqp returns converted query
        self.evalReplacements = {}

        self.dummyStringCounter = 0

    def createUniqueDummyString(self):

        dummyEvalString = "float(dummyEvalString%s)"\
            % (str(self.dummyStringCounter))
        self.dummyStringCounter += 1

        return (dummyEvalString)

    def insertDummyStringsInt(self, listOfLines):

        newListOfLines = []

        for line in listOfLines:
            replacementInfo = None
            lineToInsert = None
            if ("eval" in line):
                subStrings = line.split("--")
                newLine = ""
                for idx, subString in enumerate(subStrings):
                    if (subString.startswith("eval")):
                        # there can be space in betweeen eval string
                        # remove the keyword "eval", remove extra space, since
                        # final retina(in json form should not have space)
                        # remove the '\"' from beginning and end, json form doesnt need this
                        # finally remove any '\\' which can be present in some cases
                        # in 1.2.3 retina which should not be in 1.3 file. Examples:
                        # --eval \"eq(gene_mortality::status, \\\"dead\\\")\"
                        # --eval \"radius_earth:radius(latitude)\"
                        originalEvalString =\
                            subString.strip("eval").strip().strip("\"").replace("\\", "")
                        dummyEvalStrings = []
                        for evalStr in originalEvalString.split("&"):
                            dummyEvalString = self.createUniqueDummyString()
                            self.evalReplacements[dummyEvalString] = evalStr
                            dummyEvalStrings.append(dummyEvalString)

                        newLine += "--eval " + "\"" + "&".join(dummyEvalStrings) + "\" "
                    else:
                        if (idx != 0):
                            # add -- to all arguments not to the operator itself
                            newLine += "--"
                        newLine += subString

                lineToInsert = newLine
            else:
                lineToInsert = line

            assert(lineToInsert is not None)
            newListOfLines.append(lineToInsert)

        assert(len(newListOfLines) == len(listOfLines))
        return (newListOfLines)

    def insertDummyStrings(self, origDataFlowFileContents = ""):

        assert(isinstance(origDataFlowFileContents, str))
        assert(len(origDataFlowFileContents) > 0)
        queryStringWithDummyStrings = ""

        # operations in Xcalar query are separated by ';'
        listOfLines = origDataFlowFileContents.strip().split(";")

        newListOfLines = list(listOfLines)
        newListOfLines = self.insertDummyStringsInt(newListOfLines)

        assert(len(listOfLines) == len(newListOfLines))

        for idx, currLine in enumerate(newListOfLines):
            if (len(currLine) > 0):
                queryStringWithDummyStrings += currLine
                queryStringWithDummyStrings += ";"

        return queryStringWithDummyStrings)

    def revertDummyEvalStringsToOriginal(self, queryIn13Form):
        numOrigReplacements = len(self.evalReplacements)
        numReverts = 0

        if (numOrigReplacements == 0):
            # nothing was replaced during insertDummyStrings
            return (queryIn13Form)

        try:
            for operator in queryIn13Form:
                evalArgs = operator["args"].get("eval", None)
                if evalArgs is not None:
                    for evalArg in evalArgs:
                        if self.verbose:
                            print("%s reverted with %s" % (evalArg["evalString"], self.evalReplacements[evalArg["evalString"]]))
                        evalArg["evalString"] = self.evalReplacements[evalArg["evalString"]]
                        numReverts += 1


            assert(numReverts == numOrigReplacements)
            return (queryIn13Form)
        except:
            print("Unable to replace the values back to original eval strings")
            return (None)

    def updateQueryAndVersionsFromOutput(self, dstJsonObject, srcJsonObject):

        assert('xcalarVersion' in dstJsonObject.keys())
        assert('xcalarVersion' in srcJsonObject.keys())
        assert('dataflowVersion' in srcJsonObject.keys())
        assert('query' in dstJsonObject.keys())

        dataflowVersion = srcJsonObject['dataflowVersion']

        if ('retinaVersion' in dstJsonObject):
            del dstJsonObject['retinaVersion']
            dstJsonObject.update((('dataflowVersion', dataflowVersion),))
        else:
            assert('dataflowVersion' in dstJsonObject.keys())
            dstJsonObject['dataflowVersion'] = srcJsonObject['dataflowVersion']


        dstJsonObject['xcalarVersion'] = srcJsonObject['xcalarVersion']
        dstJsonObject['query'] = srcJsonObject['query']

    def buildDag(self, query):
        dag = {}
        for operator in query:
            opArgs = operator["args"]
            nodeName = opArgs["dest"]
            node = Node(nodeName, operator)
            dag[nodeName] = node

            source = opArgs.get("source", None)
            if source is None:
                continue
            elif isinstance(source, list):
                for parent in opArgs["source"]:
                    dag[parent].addChild(node)
                    node.addParent(dag[parent])
            else:
                dag[source].addChild(node)
                node.addParent(dag[source])

        return dag

    def parseEvalString(self, evalStr):
        lparens, rparens, comma, colon = map(pp.Suppress, "(),:")
        boolean = pp.Keyword("true") | pp.Keyword("false")
        identifier = pp.Word(pp.alphas + "_-^", pp.alphanums + "_-^").setName("identifier")
        fnName = pp.Group(pp.Optional(identifier + colon)("moduleName") + identifier("fnName"))
        integer = pp.Regex(r"[-]?\d+")
        varName = pp.Group(identifier + "::" + identifier) | identifier
        expr = pp.Forward()
        value = pp.Group(expr | integer("integer") | boolean("boolean") | pp.QuotedString('"', escChar='\\')("stringLiteral") | varName("variable"))
        expr << pp.Group(fnName("fn") + lparens + pp.Optional(pp.Group(value + pp.ZeroOrMore(comma + value))("fnArgs")) + rparens)("expr")
        if self.verbose:
            print("Parsing {}".format(evalStr))
        return expr.parseString(evalStr)

    def getFieldsRequiredFromAst(self, ast):
        fieldsRequiredSet = {}
        fnArgs = ast["expr"].get("fnArgs", None)
        if fnArgs is None:
            return {}
        for fnArg in fnArgs:
            nestedExpr = fnArg.get("expr", None)
            variable = fnArg.get("variable", None)
            if nestedExpr is not None:
                fieldsRequiredSet.update(self.getFieldsRequiredFromAst(fnArg))
            elif variable is not None:
                fieldsRequiredSet["".join(variable)] = True
        return fieldsRequiredSet

    def getFieldsRequired(self, node):
        fieldsRequiredSet = {}
        operator = node.operator
        if operator["operation"] == "XcalarApiBulkLoad":
            return fieldsRequiredSet

        evalStrs = operator["args"].get("eval", None)
        if evalStrs is not None:
            for evalStr in evalStrs:
                if (self.verbose):
                    print("Trying to parse %s" % evalStr["evalString"])
                ast = self.parseEvalString(evalStr["evalString"])
                fieldsRequiredSet.update(self.getFieldsRequiredFromAst(ast))

        elif operator["operation"] == "XcalarApiExport":
            for column in operator["args"]["columns"]:
                fieldsRequiredSet[column["columnName"]] = True
        elif operator["operation"] == "XcalarApiIndex":
            for key in operator["args"]["key"]:
                if key["name"] != "xcalarRecordNum":
                    fieldsRequiredSet[key["name"]] = True

        return fieldsRequiredSet

    def getFieldsProduced(self, node):
        fieldsProducedSet = {}
        operator = node.operator
        if operator["operation"] == "XcalarApiFilter":
            return fieldsProducedSet

        evalStrs = operator["args"].get("eval", None)
        if evalStrs is not None:
            for evalStr in evalStrs:
                if operator["operation"] == "XcalarApiAggregate":
                    fieldsProducedSet["^%s" % (operator["args"]["dest"])] = True
                else:
                    fieldsProducedSet[evalStr["newField"]] = True

            if operator["operation"] == "XcalarApiGroupBy":
                newKeyField = operator["args"].get("newKeyField", None)
                if newKeyField is not None:
                    fieldsProducedSet[newKeyField] = True

        elif operator["operation"] == "XcalarApiIndex":
            for key in operator["args"]["key"]:
                fieldsProducedSet[key["keyFieldName"]] = True
        elif operator["operation"] == "XcalarApiJoin":
            for joinRenameColumns in operator["args"]["columns"]:
                for joinRenameColumn in joinRenameColumns:
                    fieldsProducedSet[joinRenameColumn["destColumn"]] = True
        elif operator["operation"] == "XcalarApiGetRowNum":
            fieldsProducedSet[operator["args"]["newField"]] = True

        return fieldsProducedSet

    def fieldsRequiredAnalysis(self, dag, finalTablesIn):
        finalTables = { table["name"]: table for table in finalTablesIn }
        terminalNodes = [dag[nodeName] for nodeName in dag.keys() if len(dag[nodeName].children) == 0]
        queue = []
        for terminalNode in terminalNodes:
            if terminalNode.operator["args"]["dest"] in finalTables:
                finalTable = finalTables[terminalNode.operator["args"]["dest"]]
                for column in finalTable["columns"]:
                    terminalNode.fieldsRequiredSet[column["columnName"]] = True
            queue.insert(0, terminalNode)

        while len(queue) > 0:
            node = queue.pop()

            fieldsProduced = self.getFieldsProduced(node)
            node.fieldsProducedSet.update(fieldsProduced)

            if node.operator["operation"] == "XcalarApiGroupBy" and not node.operator["args"]["includeSample"]:
                fieldsRequired = {}
            elif node.operator["operation"] == "XcalarApiProject":
                fieldsRequired = { key: True for key in node.operator["args"]["columns"] }
            elif node.operator["operation"] == "XcalarApiIndex" and len(node.operator["args"]["prefix"].strip()) > 0:
                prefix = node.operator["args"]["prefix"].strip()
                fieldsRequired = { key: True for key in node.fieldsRequiredSet.keys() if key.startswith(prefix) }
            else:
                fieldsRequired = { key: True for key in node.fieldsRequiredSet.keys() - node.fieldsProducedSet.keys() }

            fieldsRequired.update(self.getFieldsRequired(node))
            node.fieldsRequiredSet = fieldsRequired

            for parent in node.parents:
                parent.fieldsRequiredSet.update(fieldsRequired)
                parent.numChildrenProcessed += 1
                if parent.numChildrenProcessed == len(parent.children):
                    queue.insert(0, parent)

    def pruneDeadFields(self, dag):
        startNodes = [dag[nodeName] for nodeName in dag.keys() if len(dag[nodeName].parents) == 0]
        queue = []
        for startNode in startNodes:
            queue.insert(0, startNode)

        while len(queue) > 0:
            node = queue.pop()

            for child in node.children:
                if child not in queue:
                    queue.insert(0, child)

            if (len(node.parents) == 0):
                continue

            fieldsAvailable = {}
            for parent in node.parents:
                fieldsAvailable.update(parent.fieldsRequiredSet)
                fieldsAvailable.update(parent.fieldsProducedSet)

            if node.operator["operation"] == "XcalarApiGroupBy" and not node.operator["args"]["includeSample"]:
                fieldsRequired = {}
                fieldsRequired.update({ key["newField"]: True for key in node.operator["args"]["eval"] })
                fieldsRequired[node.operator["args"]["newKeyField"]] = True
            else:
                fieldsRequired = { key: True for key in node.fieldsRequiredSet if key in fieldsAvailable }
            node.fieldsRequiredSet = fieldsRequired

    def indexKeyAnalysis(self, dag):
        startNodes = [dag[nodeName] for nodeName in dag.keys() if len(dag[nodeName].parents) == 0]
        queue = []
        for startNode in startNodes:
            queue.insert(0, startNode)

        while len(queue) > 0:
            node = queue.pop(;
            for child in node.children:
                if child not in queue:
                    queue.insert(0, child)

            if (len(node.parents) == 0):
                continue

            if node.operator["operation"] == "XcalarApiIndex":
                node.indexKey = node.operator["args"]["key"]
            else:
                if isinstance(node.operator["args"]["source"], list):
                    source = node.operator["args"]["source"][0]
                else:
                    source = node.operator["args"]["source"]
                srcNode = dag[source]
                node.indexKey = srcNode.indexKey

            if node.operator["operation"] == "XcalarApiGroupBy":
                if len(node.operator["args"]["newKeyField"]) > 0:
                    node.indexKey[0]["keyFieldName"] = node.operator["args"]["newKeyField"]


    def printDag(self, dag):
        startNodes = [dag[nodeName] for nodeName in dag.keys() if len(dag[nodeName].parents) == 0]
        queue = []
        for startNode in startNodes:
            queue.insert(0, startNode)

        while len(queue) > 0:
            node = queue.pop()
            print(json.dumps(node))
            for child in node.children:
                if child not in queue:
                    queue.insert(0, child)

    def fixupJoin(self, node, operator):
        opArgs = operator["args"];
        ii = 0
        for parent in node.parents:
            fieldsAvailable = {}
            fieldsAvailable.update(parent.fieldsRequiredSet)
            fieldsAvailable.update(parent.fieldsProducedSet)

            fieldsRenamed = { field["sourceColumn"]: field for field in opArgs["columns"][ii] }

            for field in fieldsAvailable:
                tmp = field.split("::")
                if len(tmp) > 1:
                    fatptrPrefix = tmp[0]
                    if fatptrPrefix in fieldsRenamed:
                        continue
                    fieldsRenamed[fatptrPrefix] = { "sourceColumn": fatptrPrefix,
                                                    "columnType": "DfFatptr",
                                                    "destColumn": fatptrPrefix }
                else:
                    if field in fieldsRenamed:
                        continue
                    fieldsRenamed[field] = { "sourceColumn": field,
                                             "columnType": "DfUnknown",
                                             "destColumn": field }

            operator["args"]["columns"][ii] = [ fieldsRenamed[field] for field in fieldsRenamed.keys() ]
            operator["args"]["key"][ii] = [ key["keyFieldName"] for key in parent.indexKey ]
            ii += 1


    def fixupSchemaViaInference(self, dataflowIn):
        query = dataflowIn["query"]
        dag = self.buildDag(query)
        self.fieldsRequiredAnalysis(dag, dataflowIn["tables"])
        self.pruneDeadFields(dag)
        self.indexKeyAnalysis(dag)

        # Now we're ready to do the fix-ups
        for operator in query:
            if operator["operation"] == "XcalarApiJoin":
                node = dag[operator["args"]["dest"]]
                self.fixupJoin(node, operator)

class Node:
    def __init__(self, name, operator):
        # Using dictionary for duplicate detection
        self.fieldsProducedSet = {}
        self.fieldsRequiredSet = {}
        self.parents = []
        self.children = []
        self.indexKey = None
        self.name = name
        self.operator = operator
        self.numChildrenProcessed = 0

    def to_json(self):
        return { "operation": self.operator["operation"], "name": self.name,
                 "evalStrings": self.operator["args"].get("eval", None),
                 "parents": [ parent.name for parent in self.parents ],
                 "children": [ child.name for child in self.children ],
                 "fieldsRequired": [key for key in self.fieldsRequiredSet.keys()],
                 "fieldsProduced": [key for key in self.fieldsProducedSet.keys()] }

    def addParent(self, node):
        self.parents.append(node)

    def addChild(self, node):
        self.children.append(node)
