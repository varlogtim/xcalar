# Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
import json
from json import JSONEncoder

from .XcEvalStringHelper import XcEvalStringHelper


def _default(self, obj):
    return getattr(obj.__class__, "to_json", _default.default)(obj)


_default.default = JSONEncoder().default
JSONEncoder.default = _default


class Node:
    def __init__(self, name, operator):
        # Using dictionary for duplicate detection
        # key: field name
        # value: nodes actually requiring field
        self.fieldsRequiredSet = {}

        self.fieldsProducedSet = {}
        self.keys = []
        self.parents = []
        self.children = []
        self.name = name
        self.operator = operator
        self.numChildrenProcessed = 0

    def to_json(self):
        return {
            "operation": self.operator["operation"],
            "name": self.name,
            "evalStrings": self.operator["args"].get("eval", None),
            "parents": [parent.name for parent in self.parents],
            "children": [child.name for child in self.children],
            "keys": [key["keyFieldName"] for key in self.keys],
            "fieldsRequired": [key for key in self.fieldsRequiredSet.keys()],
            "fieldsProduced": [key for key in self.fieldsProducedSet.keys()]
        }

    def addParent(self, node):
        self.parents.append(node)

    def addChild(self, node):
        self.children.append(node)

    def applyRenameMap(self, sourceIdx, colName):
        columns = self.operator["args"].get("columns", None)
        if columns and self.operator["operation"] != "XcalarApiProject":
            if isinstance(columns[0], list):
                renameMap = columns[sourceIdx]
            else:
                renameMap = columns

            for col in renameMap:
                if "sourceColumn" in col and col["sourceColumn"] == colName:
                    return col["destColumn"]

        return colName


class Graph:
    def __init__(self, query):
        self.dag = {}
        for operator in query:
            opArgs = operator["args"]
            nodeName = opArgs["dest"]
            node = Node(nodeName, operator)
            self.dag[nodeName] = node

            source = opArgs.get("source", None)
            if source is None:
                continue
            elif isinstance(source, list):
                for parent in opArgs["source"]:
                    if parent in self.dag:
                        self.dag[parent].addChild(node)
                        node.addParent(self.dag[parent])
            else:
                if source in self.dag:
                    self.dag[source].addChild(node)
                    node.addParent(self.dag[source])

    def removeNode(self, nodeName):
        node = self.dag[nodeName]
        print("removing " + nodeName + " " + node.operator["operation"])
        # can only remove nodes with 1 or fewer parents
        assert (len(node.parents) <= 1)

        if (len(node.parents) == 1):
            # fix up the children links to point to new parent

            sourceName = node.operator["args"]["source"]
            destName = node.operator["args"]["dest"]
            parent = node.parents[0]

            for child in node.children:
                source = child.operator["args"]["source"]
                if isinstance(source, list):
                    child.operator["args"]["source"] = [
                        sourceName if x == destName else x for x in source
                    ]
                else:
                    assert (child.operator["args"]["source"] == destName)
                    child.operator["args"]["source"] = sourceName

                idx = child.parents.index(node)
                child.parents[idx] = parent

        else:
            for child in node.children:
                child.parents.remove(node)

        del self.dag[nodeName]

    def populateFieldsOfInterest(self, finalTables):
        evalHelper = XcEvalStringHelper()
        evalHelper.fieldsRequiredAnalysis(self.dag, finalTables)
        evalHelper.pruneDeadFields(self.dag)
        evalHelper.indexKeyAnalysis(self.dag)

    def printDag(self):
        startNodes = [
            self.dag[nodeName] for nodeName in self.dag.keys()
            if len(self.dag[nodeName].parents) == 0
        ]
        queue = []
        for startNode in startNodes:
            queue.insert(0, startNode)

        while len(queue) > 0:
            node = queue.pop()
            print(json.dumps(node))
            for child in node.children:
                if child not in queue:
                    queue.insert(0, child)

    def toQueryDict(self):
        query = []
        for nodeName, node in self.dag.items():
            query.append(node.operator)

        return query


def updateFieldsRequired(dag, finalTables):
    terminalNodes = [
        dag[nodeName] for nodeName in dag.keys()
        if len(dag[nodeName].children) == 0
    ]
    queue = []
    for terminalNode in terminalNodes:
        if terminalNode.operator["args"]["dest"] in finalTables:
            finalTable = finalTables[terminalNode.operator["args"]["dest"]]
            for column in finalTable["columns"]:
                terminalNode.fieldsRequiredSet[column["columnName"]] = [
                    terminalNode
                ]

        queue.insert(0, terminalNode)

    while len(queue) > 0:
        node = queue.pop()


def main(inObj):
    graph = Graph(inObj["query"])
    tables = inObj["finalTables"]

    graph.populateFieldsOfInterest(tables)
