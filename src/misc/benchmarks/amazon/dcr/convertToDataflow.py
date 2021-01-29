import argparse
import json
import tarfile
import hashlib

from json import JSONEncoder

import pyparsing as pp
import sys

from xcalar.compute.api.XcalarApi import XcalarApi
from xcalar.compute.api.Session import Session

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
    
    def buildDag(self, query):
        dag = {}
        for operator in query:
            opArgs = operator["args"]
            nodeName = opArgs["dest"]
            node = Node(nodeName, operator)
            dag[nodeName] = node

            source = opArgs.get("source", None)
            if source is None or operator["operation"] == "XcalarApiSynthesize":
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
        floatLiteral = pp.Regex(r"[-]?\d+\.\d+")
        varName = pp.Group(identifier + "::" + identifier) | identifier
        expr = pp.Forward()
        value = pp.Group(expr | floatLiteral("float") | integer("integer") | boolean("boolean") | pp.QuotedString('"', escChar='\\')("stringLiteral") | varName("variable"))
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
                keyFieldName = key["keyFieldName"]
                if len(keyFieldName) == 0:
                    keyFieldName = key["name"]
                fieldsProducedSet[keyFieldName] = True
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
            node = queue.pop();
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
        key = operator["args"].get("key", None)
        if key is None:
            operator["args"]["key"] = []

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
            if self.verbose:
                print(json.dumps(operator))
            value = [ key["keyFieldName"] for key in parent.indexKey ]
            if len(operator["args"]["key"]) <= ii:
                operator["args"]["key"].append(value)
            else:
                operator["args"]["key"][ii] = value
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

if __name__ == "__main__":
    argParser = argparse.ArgumentParser(description="Convert raw xcalar queries into batch dataflow")
    argParser.add_argument('--inputFile', '-f', help="Path to file containing raw Xcalar queries", required=True)
    argParser.add_argument('--destTableFile', '-d', help="Path to file containing JSON description of destination tables", required=True)
    argParser.add_argument('--xcalar', '-x', help="Url to xcalar mgmtd instance", required=True, default="localhost")
    argParser.add_argument('--user', '-u', help="Xcalar User", required=True, default="admin")
    argParser.add_argument('--session', '-s', help="Name of session", required=True)

    args = argParser.parse_args()

    inFile = args.inputFile
    xcalarInstance = args.xcalar
    userId = args.user
    sessionName = args.session
    destTableFile = args.destTableFile

    mgmtdUrl = "http://%s:9090/thrift/service/XcalarApiService/" % xcalarInstance.rstrip()

    xcApi = XcalarApi(mgmtdUrl)
    userIdUnique = int(hashlib.md5(userId.encode("UTF-8")).hexdigest()[:5], 16) + 4000000

    session = Session(xcApi, userId, userId, userIdUnique, True, sessionName=sessionName)
    xcApi.setSession(session)

    listTablesOut = xcApi.listTable("*")
    tables = { node.name.split("#")[0]: node.name for node in listTablesOut.nodeInfo }

    with open(inFile) as f:
        query = json.load(f)

    with open(destTableFile) as f:
        dstTables = json.load(f)

    srcTables = {}
    for op in query:
        opArgs = op["args"]
        if isinstance(opArgs["source"], list):
            for src in opArgs["source"]:
                if src not in srcTables:
                    srcTables[src] = True
        else:
            if opArgs["source"] not in srcTables:
                srcTables[opArgs["source"]] = True

        srcTables[opArgs["dest"]] = False

    dataflow = {}
    sources = [ srcTable for srcTable in srcTables.keys() if srcTables[srcTable] ]

    for source in sources:
        canonicalName = source.split("#")[0]
        if canonicalName not in tables:
            raise ValueError("Couldn't find %s" % canonicalName)

        print("%s => %s" % (source, tables[canonicalName]))
        query.insert(0, { "operation": "XcalarApiSynthesize",
                          "comment": "",
                          "args": {
                              "source": "%s" % tables[canonicalName],
                              "dest": source,
                              "sameSession": False,
                              "columns": []
                           }
                         })

    for dstTable in dstTables:
        exportColumns = [ { "columnName": col["columnName"],
                            "headerName": col["headerAlias"] } for col in dstTable["columns"] ]
        query.append( { "operation": "XcalarApiExport",
                        "comment": "",
                        "args": {
                            "source": dstTable["name"],
                            "fileName": "export-%s.csv" % dstTable["name"],
                            "targetName": "Default",
                            "targetType": "file",
                            "dest": ".XcalarLRQExport.%s" % dstTable["name"],
                            "columns": exportColumns,
                            "splitRule": "none",
                            "splitSize": 0,
                            "splitNumFiles": 0,
                            "headerType": "every",
                            "createRule": "createOnly",
                            "sorted": True,
                            "format": "csv",
                            "fieldDelim": ",",
                            "recordDelim": "\n",
                            "quoteDelim": "\""
                        }
                    })

    dataflow["query"] = query
    dataflow["schema hints"] = []
    dataflow["udfs"] = []
    dataflow["tables"] = dstTables
    dataflow["xcalarVersion"] = "1.3.0-1488-xcalardev-ea52447a-d5a44043"
    dataflow["dataflowVersion"] = 1

    evalStringHelper = XcEvalStringHelper(verbose=False)
    evalStringHelper.fixupSchemaViaInference(dataflow)

    with open("dataflowInfo.json", "w") as fp:
        fp.write(json.dumps(dataflow))

    with tarfile.open("dcr.tar.gz", "w:gz") as tar:
        tar.add("dataflowInfo.json")

