#
# Web server to process Grafana requests for stats from the SimpleJson Grafana data source.
# This code needs to be moved to the Xcalar web server.
#

from http import server
import time
import calendar
import json
import argparse
import os
import requests
import uuid
import logging

from xcalar.external.LegacyApi.Operators import *
from xcalar.external.client import Client
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.session import Session

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)-5s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

class ColumnResult:
    def __init__(self, name):
        self.name = name
        self.splitName = self.name.split('_')
        self.realColumnName = self.splitName[0]
        self.lastComp = self.splitName[len(self.splitName) - 1]
        self.result = '{ "target" : "' + name + '", "datapoints" : ['
        self.needComma = False
        self.lastValue = None
        self.lastTimestamp = 0

    #
    # Generate a datapoint for a row.
    #
    def Update(self, rowIterator, metricMap):
        if (self.needComma):
            self.result += ', '
        self.needComma = True
        self.result += '['
        value = rowIterator.getValue()
        timestamp = rowIterator.getTimestamp()
        if (len(self.splitName) == 2):
            #
            # This is the base stat (statname_node#)
            #
            self.result += str(value)
        elif (self.lastValue == None):
            #
            # There is no previous stat so just give a result of 0.
            #
            self.result += '0'
        else:
            #
            # Generate the right kind of value based on a diff with the previous value
            # and possibly the amount of time that has elapsed.
            #
            try:
                curValue = float(value)
            except:
                curValue = int(value)
            try:
                lastValue = float(self.lastValue)
            except:
                lastValue = int(self.lastValue)
            valueDiff = curValue - lastValue
            timeDiff = int(timestamp) - int(self.lastTimestamp)
            if (self.lastComp.lower().endswith('persecond')):
                self.result += str(valueDiff / (timeDiff / 1000000))
            elif (self.lastComp == 'percentOfMax'):
                # Get the maximum value for this stat.
                maxName = rowIterator.getMaxName() + '_' + str(rowIterator.getNode())
                try:
                    maxValue = float(metricMap[maxName])
                except:
                    maxValue = int(metricMap[maxName])
                if (rowIterator.getUnits() == "MicroSeconds"):
                    #
                    # If the units are microseconds then we assume that we want utilization over the interval
                    #
                    seconds = timeDiff / 1000000
                    self.result += str(((curValue - lastValue) / (maxValue * seconds)) * 100)
                else:
                    #
                    # If the units are bytes then we just want the percent of the resource that is being used.
                    #
                    self.result += str((curValue / maxValue) * 100)
            elif (self.lastComp == 'diff'):
                self.result += str(valueDiff)
            else:
                logger.error("Unexpected", self.splitName[1])
                assert False
        self.result += ',' + str(int(timestamp / 1000)) + ']'
        self.lastTimestamp = timestamp
        self.lastValue = value

    def GetResult(self):
        return self.result + "] }"

class MyHTTPRequestHandler(server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_head(0)

    def do_HEAD(self):
        self.send_head(0)

    def parseTimeString(self, timeStr):
        timeStr = timeStr.replace('-', ':')
        timeStr = timeStr.replace('T', ':')
        timeStr = timeStr.replace('.', ':')
        timeStr = timeStr[0 : len(timeStr) - 1]
        parts = timeStr.split(':')
        timeStr = parts[2] + ' ' + self.httpSession.months[parts[1]] + ' ' + str(int(parts[0]) - 2000) + ' ' + parts[3] + ':' + parts[4] + ':' + parts[5]
        timeStruct = time.strptime(timeStr, "%d %b %y %H:%M:%S")
        tf = calendar.timegm(timeStruct)
        return int(tf) * 1000000 + int(parts[6]) * 1000

    def fetchRows(self, columns, timestampMin, timestampMax, frequency):
        #
        # Generate a query to get stats only in the time interval and only for the nodes and stats that are needed.
        #
        sqlQuery = "SELECT * FROM " + self.httpSession.imdTableName
        sqlQuery += " WHERE TIMESTAMP <= " + str(timestampMax) + " AND TIMESTAMP >= " + str(timestampMin)
        sqlQuery += " AND ("
        needOr = False
        nodeSet = set()
        columnResults = {}
        for column in columns:
            columnResults[column] = ColumnResult(column)
            splitName = column.split('_')
            nodeId = int(splitName[1])
            nodeSet.add(nodeId)
            #
            # OR in the name of the stat we want.
            #
            if (needOr):
                sqlQuery += " OR "
            needOr = True
            sqlQuery += "(NAME == '" + splitName[0] + "')"
        sqlQuery += ") AND ("
        needOr = False
        for node in nodeSet:
            #
            # OR in each node that we want.
            #
            if (needOr):
                sqlQuery += " OR "
            needOr = True
            sqlQuery += "(NODE == " + str(node) + ")"
        sqlQuery += ")"
        sqlQuery += " ORDER BY TIMESTAMP"

        rowIterator = self.httpSession.executeSql(sqlQuery)

        while (rowIterator.getNext()):
            for column in columns:
                splitName = column.split('_')
                realColumnName = splitName[0]
                nodeId = int(splitName[1])
                columnResult = columnResults[column]
                if (nodeId == rowIterator.getNode() and realColumnName == rowIterator.getName()):
                    timestamp = rowIterator.getTimestamp()
                    if (timestamp == columnResult.lastTimestamp or timestamp - columnResult.lastTimestamp > frequency):
                        columnResult.Update(rowIterator, self.httpSession.metricMap)
        result = '[ '
        targetCount = 0
        for columnResult in columnResults.values():
            if (targetCount > 0):
                result += ', '
            targetCount += 1
            result += columnResult.GetResult()
        result += ' ]'

        return result

    def do_POST(self):
        logger.info("do_POST: ""PATH=", self.path, " REQUEST=", self.requestline)
        contentLen = int(self.headers.get('Content-Length'))
        postBody = self.rfile.read(contentLen)
        logger.info("BODY=", postBody)
        if (self.path == "/search"):
            #
            # Grafana issues a "search" to get the list of available metrics
            #
            result = self.httpSession.metrics
        elif (self.path == "/query"):
            #
            # Grafana issues a "query" to get time series data points for metrics.  It provides a list of
            # targets which are the metric names, a timestamp range, and the interval in milliseconds that it
            # wants stats.
            #
            jsonString = postBody.decode("utf-8")
            bodyObj = json.loads(jsonString)

            fromTS = bodyObj["range"]["from"]
            toTS = bodyObj["range"]["to"]
            targets = bodyObj["targets"]
            targetNames = []
            for target in targets:
                targetNames.append(target["target"])
            result = self.fetchRows(targetNames, self.parseTimeString(fromTS), self.parseTimeString(toTS), bodyObj["intervalMs"] * 1000)
        self.send_head(len(result))
        self.wfile.write(bytes(result, 'utf-8'))

    def send_head(self, contentLength):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.send_header("Content-Length", contentLength)
        self.end_headers()

class http_server:
    def __init__(self, httpSession):
        MyHTTPRequestHandler.httpSession = httpSession
        self.server = server.HTTPServer(('', httpSession.port), MyHTTPRequestHandler)
        self.server.serve_forever()

class HTTPSession:
    def __init__(self, imdTableName, addDiffs, port):
        os.environ["XLR_PYSDK_VERIFY_SSL_CERT"] = "false"
        clientSecrets = {'xiusername': 'admin', 'xipassword': 'admin' }
        login_url = "http://localhost:12124/login"
        self.headers = {u'content-type': u'application/json'}
        self.apiSession = requests.Session()
        self.port = port
        res = self.apiSession.post(login_url, headers=self.headers, data = json.dumps(clientSecrets))

        self.imdTableName = imdTableName
        self.months = {
            "01" : "Jan",
            "02" : "Feb",
            "03" : "Mar",
            "04" : "Apr",
            "05" : "May",
            "06" : "Jun",
            "07" : "Jul",
            "08" : "Aug",
            "09" : "Sep",
            "10" : "Oct",
            "11" : "Nov",
            "12" : "Dec"
        }
        self.maxStat = {}

        #
        # Do one query to get all one round of stats for each node so we can deduce the metrics
        # and get the maximum values for resources.
        #
        sqlQuery = "SELECT * FROM " + self.imdTableName + " ORDER BY TIMESTAMP"
        rowIterator = self.executeSql(sqlQuery)

        self.metricMap = {}
        maxMap = {}
        nodeSet = set()
        firstTimestamp = 0

        while (rowIterator.getNext()):
            node = rowIterator.getNode()
            timestamp = rowIterator.getTimestamp()
            if (timestamp != firstTimestamp):
                if (firstTimestamp != 0 and node in nodeSet):
                    #
                    # If we've seen this node once already for a set of timestamps then assume that we
                    # seen all nodes by now since we are on the second go around.
                    # TODO This is an optimistic assumption.  If there is clock skew on the nodes then
                    #      its possible that we can get the stats twice for one node and not have seen
                    #      all nodes.
                    break
                firstTimestamp = timestamp
            nodeSet.add(node)
            #
            # We provide multiple metrics per stat name per node:
            # - statname_node#                  The value of the stat
            # - statname_node#_diff             The difference between the current value and the previous value
            # - statname_node#_percentOfMax     The percent of a maximum value for the stat if there is one
            # - statname_node#_bytesPerSecond   Bytes per second if there is no max value and the units are Bytes
            # - statname_node#_opsPerSecond     Operations per second if the units are Operations
            # - statname_node#_perSecond        Count per second if the units are Count
            #
            metricName = rowIterator.getName() + '_' + str(node)
            #
            # We want to keep track of all values in case this value is referenced by other stats
            # as there max possible value.
            #
            self.metricMap[metricName] = rowIterator.getValue()
            if (rowIterator.getMaxName() != "Constant"):
                units = rowIterator.getUnits()
                if (addDiffs):
                    self.metricMap[metricName + '_diff'] = True
                if (len(rowIterator.getMaxName()) > 0):
                    #
                    # This stat has provided the name of another stat that provides the maximum possible value for this stat.
                    # In this case we will provide the _percentOfMax stat.  We also need to remove other automatically generated
                    # metrics for the maximum value stat
                    #
                    maxMap[rowIterator.getMaxName() + '_' + str(node)] = True
                    self.metricMap[metricName + '_percentOfMax'] = True
                elif (units == "Bytes"):
                    #
                    # Assume if the Units are bytes and there is no max value that we want to
                    # compute bytes per second. Not necessarily true. Works for Network stats.
                    #
                    self.metricMap[metricName + '_bytesPerSecond'] = True
                elif (units == "Operations"):
                    #
                    # Assume if the Units are Operatonis and there is no max value that we want to
                    # compute operations per second. Not necessarily true. Works for Network stats.
                    #
                    self.metricMap[metricName + '_opsPerSecond'] = True
                elif (units == "Count"):
                    #
                    # Assume if the Units are Count and there is no max value that we want to
                    # compute count per second.
                    #
                    self.metricMap[metricName + '_perSecond'] = True

        self.metrics = ''
        firstMetric = True
        for key in self.metricMap.keys():
            if (len(self.metrics) == 0):
                self.metrics = '[ "'
            else:
                self.metrics += ', "'
            self.metrics += key + '"'
        self.metrics += ']'
        logger.info("METRICS", self.metrics)

    def executeSql(self, sqlQuery):
        url = "http://localhost:12124/xcsql/queryWithPublishedTables"
        headers = {u'content-type': u'application/json'}
        r = uuid.uuid4().hex
        payload = {
            "queryString": sqlQuery,
            "execid": r,
            "sessionId": "sqlapitest" + r,
            "checkTime": "25",
            "queryName": "sqltest"+ r,
            "usePaging": "true"
        }
        res = self.apiSession.post(url, headers=headers, data=json.dumps(payload))
        return RowIterator(self.headers, self.apiSession, res.json())

class RowIterator:
    def __init__(self, headers, apiSession, queryResult):
        self.headers = headers
        self.apiSession = apiSession
        self.resultSetId = queryResult['resultSetId']
        self.totalRows = queryResult['totalRows']
        self.renameMap = queryResult['renameMap']
        self.schema = queryResult['schema']
        self.groupSize = 10
        colNum = 0
        for col in self.schema:
            if (col == { 'P-VALUE' : 'string' }):
                self.valueColumn = colNum
            elif (col == { 'NODE' : 'integer' }):
                self.nodeColumn = colNum
            elif (col == { 'TIMESTAMP' : 'integer' }):
                self.timestampColumn = colNum
            elif (col == { 'NAME' : 'string' }):
                self.nameColumn = colNum
            elif (col == { 'UNITS' : 'string' }):
                self.unitsColumn = colNum
            elif (col == { 'MAXNAME' : 'string' }):
                self.maxNameColumn = colNum
            colNum += 1
        self.batchIndex = 0
        self.batchSize = 0
        self.nextRowToFetch = 0

    def getNext(self):
        if (self.batchIndex < self.batchSize):
            self.row = self.batch[self.batchIndex]
            self.batchIndex += 1
            return True
        elif (self.nextRowToFetch == self.totalRows):
            return False
        self.batchSize = self.totalRows - self.nextRowToFetch
        if (self.batchSize > self.groupSize):
            self.batchSize = self.groupSize
        payload = {
            "resultSetId":  self.resultSetId,
            "rowPosition": self.nextRowToFetch,
            "rowsToFetch": self.batchSize,
            "schema": json.dumps(self.schema),
            "renameMap": json.dumps(self.renameMap)
        }
        url = "http://localhost:12124/xcsql/result"
        res = self.apiSession.post(url, headers=self.headers, data=json.dumps(payload))
        self.nextRowToFetch += self.batchSize
        self.batch = res.json()
        self.row = self.batch[0]
        self.batchIndex = 1
        return True

    def getValue(self):
        return self.row[self.valueColumn]

    def getNode(self):
        return self.row[self.nodeColumn]

    def getTimestamp(self):
        return self.row[self.timestampColumn]

    def getName(self):
        return self.row[self.nameColumn]

    def getUnits(self):
        return self.row[self.unitsColumn]

    def getMaxName(self):
        return self.row[self.maxNameColumn]

    def getRowCount(self):
        return self.totalRows

parser = argparse.ArgumentParser()
parser.add_argument("--imdtable")
parser.add_argument("--adddiffs")
parser.add_argument("--port", nargs='?', type=int, default=5123)
args = parser.parse_args()

class main:
    def __init__(self):
        self.server = http_server(HTTPSession(args.imdtable, args.adddiffs, args.port))

if __name__ == '__main__':
    m = main()
