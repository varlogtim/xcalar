#!/usr/bin/env python3.6

import sys


def poorManCsvToJson(fileName, inStream):
    for line in inStream.readlines():
        line = line.decode("utf-8")
        record = {}
        for colNum, col in enumerate(line.split(",")):
            record["col{}".format(colNum)] = col
        record["fileName"] = fileName
        yield record


def main():
    from io import StringIO
    testStr = ("John,1,Donny,Darko,1,3\n"
               "John,1,Existenze,2,5\n"
               "John,1,Sex and the city,3,4\n"
               "John,1,The Hours,4,2.5\n")
    for record in poorManCsvToJson(__file__, StringIO(testStr)):
        print(record)
    sys.exit(0)


if __name__ == "__main__":
    main()
