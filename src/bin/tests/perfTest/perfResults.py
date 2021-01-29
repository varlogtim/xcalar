#!/usr/bin/env python2.7
"""
    Analyzes performance tests to determine a possible performance regression.
    This file contains the 'data analysis' half of performance regression
    testing. It achieves the following:
        1. Read test data from user SQLite database
        2. Analyze and aggregate past test results
        3. Compare most recent results to past results
        4. Pretty print an analysis table
    Related files:
        perfResults.py: For the gathering half
        dbUpdate.sql: Specifies the database schema for the outputs of this test
"""
import argparse
import sys
import sqlite3
from texttable import Texttable


def info(s):
    print(s, file=sys.stderr)


class CommandLog(object):
    def __init__(self):
        self.commands = []

    def addCommand(self, commandStr, timeElapsed):
        self.commands.append((commandStr, timeElapsed))

    def readFromDb(self, conn, testid, numRecent):
        """
        Returns an array of numRecent test results for test testid, averaged on
        a per-suite-run basis.
        The array is from most recent to least recent
        """
        result = []
        with conn:
            cursor = conn.cursor()
            for age in range(numRecent):
                cursor.execute(
                    """
                    SELECT avg(secTaken),
                        cmdstring
                    FROM commands
                    LEFT JOIN testRun ON commands.runid = testRun.runid
                    WHERE testRun.suiteid IN (SELECT suiteid
                                    FROM suiteRun
                                    ORDER BY suiteRun.timestamp DESC
                                    LIMIT 1 OFFSET :age)
                    AND testRun.testid = :testid
                    GROUP BY testRun.suiteid, commands.stepnum
                    ORDER BY stepnum
                        """, {
                        "testid": testid,
                        "age": age
                    })
                dbcmds = cursor.fetchall()
                cmdlog = CommandLog()
                for cmd in dbcmds:
                    cmdlog.addCommand(cmd[1], cmd[0])
                result.append(cmdlog)
        return result

    def average(self, cmdLogs):
        retLog = CommandLog()
        if not cmdLogs:
            return retLog
        log0 = cmdLogs[0]
        if not log0.commands:
            return retLog
        validLogs = [
            cmdLog for cmdLog in cmdLogs if cmdLog and cmdLog.commands
        ]
        for op, cmd in enumerate(log0.commands):
            sumTime = sum([
                cmdLog.commands[op][1] for cmdLog in validLogs
                if cmdLog.commands
            ])
            avgTime = sumTime / len(validLogs)
            retLog.addCommand(cmd[1], avgTime)
        return retLog


class RegressionAnalysis(object):
    def __init__(self, resultDb):
        print("Performance Test Database: '{}'".format(resultDb))
        self.resultDb = resultDb

        self.conn = sqlite3.connect(self.resultDb)
        self.conn.execute("PRAGMA foreign_keys=on")
        return

    def getTestsFromSuite(self, suiteAge):
        """
        Get a list of all the tests in the suiteAge'th newest suite run
        returns tuples (testid, testname)
        """
        testids = None
        with self.conn:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                    SELECT DISTINCT perfTest.testid, perfTest.testname
                    FROM perfTest
                    JOIN testRun ON perfTest.testid = testRun.testid
                    WHERE testRun.suiteid IN (SELECT suiteid
                                    FROM suiteRun
                                    ORDER BY suiteRun.timestamp DESC
                                    LIMIT 1 OFFSET :age)
                    """, {"age": suiteAge})
            testids = cursor.fetchall()
        return testids

    def formatCell(self, stepnum, baseCmds, thisCmds):
        if not thisCmds or not thisCmds.commands:
            return "N/A\nN/A"
        else:
            thisVal = thisCmds.commands[stepnum][1]
            baseVal = baseCmds.commands[stepnum][1]
            # some operations can be super fast and the time
            # calculated can be zero. Handle that case here
            if baseVal == 0:
                baseVal = 0.01
            return "{thisVal:4.2f}sec\n{speedup:4.2f}x".format(
                thisVal=thisVal, speedup=thisVal / baseVal)

    def detCmdPass(self, stepnum, thisCmds, cmpCmds):
        # Allow some flat overhead, so a change from 0.1s to 0.2s doesn't fail
        threshold = 0.8
        overhead = 1.0
        if not cmpCmds or not cmpCmds.commands:
            return True
        if not thisCmds or not thisCmds.commands:
            return False
        thisVal = thisCmds.commands[stepnum][1]
        cmpVal = cmpCmds.commands[stepnum][1]
        return thisVal < (cmpVal + overhead) * (1 / threshold)

    def detTestPass(self, thisCmds, refCmdList, shouldPrint):
        passed = True
        for stepnum, cmd in enumerate(thisCmds.commands):
            for refCmds in refCmdList:
                if not refCmds or not refCmds.commands:
                    continue
                thisPass = self.detCmdPass(stepnum, thisCmds, refCmds)
                if not thisPass:
                    passed = False
                    if shouldPrint:
                        print("Failed on operation {}:\n  {}".format(
                            stepnum, cmd[0]))
        return passed

    def getResultRow(self, stepnum, todayCmds, refCmdList):
        """
        todayCmds is a list of commands
        refCmdList is a list of lists of commands
        """
        regPass = all([
            self.detCmdPass(stepnum, todayCmds, refCmds)
            for refCmds in refCmdList
        ])
        opName = "UNKNOWN"
        cmdTokens = todayCmds.commands[stepnum][0].split()
        if cmdTokens:
            opName = cmdTokens[0]
        row = [
            "{}\n{}".format(stepnum, "OK" if regPass else "REGRESSION"),
            opName,
            self.formatCell(stepnum, todayCmds, todayCmds)
        ]
        for cmpCmds in refCmdList:
            row.append(self.formatCell(stepnum, todayCmds, cmpCmds))
        return row

    def printResultTable(self, testname, cmdlogs):
        """
        Prints the result table and returns whether the test passed
        """
        print("---------Test {}----------".format(testname))
        table = Texttable()
        table.add_row([
            "Seq Number", "Command", "Today", "Yesterday", "2 Days Ago",
            "10 day avg"
        ])
        table.set_cols_align(["l", "l", "c", "c", "c", "c"])
        table.set_cols_valign(["t", "m", "b", "b", "b", "b"])

        today = cmdlogs[0]
        yest = cmdlogs[1]
        # dby is Day Before Yesterday
        dby = cmdlogs[2]

        # Average the rest
        tenDayAvg = CommandLog().average(cmdlogs[1:])
        refCmds = [yest, dby, tenDayAvg]
        for stepnum, cmd in enumerate(today.commands):
            row = self.getResultRow(stepnum, today, refCmds)
            table.add_row(row)
        info(table.draw())

        return self.detTestPass(today, refCmds, True)

    def getLastShas(self, numRecent):
        gitshas = None
        with self.conn:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT gitsha
                FROM suiteRun
                ORDER BY suiteRun.timestamp DESC
                LIMIT :topnum
                """, {"topnum": numRecent})
            gitshas = cursor.fetchall()
        flatShas = [record[0] for record in gitshas]
        return flatShas

    def printShaReport(self, numRecent):
        print("Checking against last {} runs with SHAs:".format(numRecent))
        for ii, sha in enumerate(self.getLastShas(numRecent)):
            shaStr = "No SHA" if not sha else sha
            if ii == 0:
                pass
                print("  {}: {} (Run to compare)".format(ii, shaStr))
            else:
                print("  {}: {}".format(ii, shaStr))

    def report(self):
        """
        Returns whether all tests passed or not
        """
        tests = self.getTestsFromSuite(0)
        passed = []
        numRecent = 10
        print("Found {} tests in the last suite run:".format(len(tests)))
        for test in tests:
            print("  {}".format(test[1]))
        print()
        for test in tests:
            cmdlogs = CommandLog().readFromDb(self.conn, test[0], numRecent)
            self.printShaReport(numRecent)
            testPassed = self.printResultTable(test[1], cmdlogs)
            passed.append(testPassed)

        if not all(passed):
            print("PERFORMANCE REGRESSION")
            print("Tests failed ({} failures):".format(
                len(passed) - sum(passed)))
            for ii in [jj for jj in range(len(passed)) if not passed[jj]]:
                print("  {}".format(tests[ii][1]))
        else:
            print("All {} tests passed".format(len(tests)))
        return all(passed)


def main():
    parser = argparse.ArgumentParser(description="Analyze performance tests")
    parser.add_argument(
        "-r",
        "--resultDb",
        default="./perf.db",
        help="Path to stored perf test results")

    args = parser.parse_args()
    resultDir = args.resultDb

    analysis = RegressionAnalysis(resultDir)
    allPassed = analysis.report()
    if allPassed:
        return 0
    else:
        return 1


if __name__ == "__main__":
    sys.exit(main())
