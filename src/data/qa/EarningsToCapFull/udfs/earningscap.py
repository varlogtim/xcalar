#
# Import Parser and Map UDFs for EarningsToCap Dataflow
#
import bisect

# Parse Market Cap CSV
# Ignore global issuer Market Cap datafiles before 1992 because Earnings
# data is only available since Jan 1, 1992. Cast integers, floats correctly.
#
def parseMcapComma(inFile, inStream):
    headers = []
    firstRow = True

    if int(inFile.split("-")[1]) < 1992:
        return
    for line in inStream:
        line = line[:-1]
        fields = line.split(",")
        if firstRow:
            headers = fields
            firstRow = False
            continue
        record = {}

        try:
            record[headers[0]] = int(float(fields[0]))
            record[headers[1]] = int(float(fields[1]))
            record[headers[2]] = float(fields[2])
            record[headers[3]] = int(float(fields[3]))
        except ValueError:
            yield {"xcError: ": str(ValueError)}
            continue
        yield record


# Parse Earnings CSV
# Cast integers, floats correctly.
#
def parseEarningsComma(inFile, inStream):
    headers = []
    firstRow = True

    for line in inStream:
        line = line[:-1]
        fields = line.split(",")
        if firstRow:
            headers = fields
            firstRow = False
            continue
        record = {}

        try:
            record[headers[0]] = int(float(fields[0]))
            record[headers[1]] = int(float(fields[1]))
            record[headers[2]] = int(float(fields[2]))
            record[headers[3]] = int(float(fields[3]))
            record[headers[4]] = int(float(fields[4]))
        except ValueError:
            yield {"xcError: ": str(ValueError)}
            continue
        yield record

# Extract 6-digit Matlab dates from a concatenated digit field
# previously constructed by a list-aggregation.
# Return a sorted list of these 6-digit dates.
#
def sortDates(datesChain):
    dates = []
    for i in xrange(len(datesChain)/6):
        dates.append(datesChain[i*6:(i+1)*6])
    return ",".join(sorted(dates))

# Return highest Publication Date less than or equal to Analysis Date.
# But do not return any date that is more than "maxAgeYears" ago.
# Input publication date chain must be sorted for bisect to work.
# It is possible that no publication date meets this criteria.
#
def getNearestPubDate(analysisDate, sortedDates, maxYearGap):
    sortedList = sortedDates.split(",")
    idx = bisect.bisect_right(sortedList, str(analysisDate))
    if idx and analysisDate <= (int(sortedList[idx-1]) + maxYearGap * 365):
        return sortedList[idx-1]
    return "None Found"

def addOne(col):
    return str(int(col) + 1)

# Convert Matlab Date to a Gregorian Date
# Matlab Date is days since proleptic January 1, 0000.
# Ordinal is days since proleptic January 1, 0001.
#
from datetime import date, datetime

def matlab2date(matlabDate):
    try:
        convdate = date.fromordinal(matlabDate)
        return str(convdate.replace(year=convdate.year-1))
    except ValueError:
        return str(convdate.replace(day=28, month=2, year=convdate.year-1))

def date2matlab(colName, inputFormat=None):
    if inputFormat == None or inputFormat == "":
        try:
            indate = dateutil.parser.parse(colName).timetuple()
        except ValueError:
            return "Unable to detect, please retry specifying outputFormat"
        except:
            return "%s %s" % ("Unexpected error", sys.exc_info()[0])
    else:
        indate = datetime.strptime(colName, inputFormat)

    try:
        convdate = indate.replace(year = indate.year + 1)
        return convdate.toordinal()
    except ValueError:
        return str(convdate.replace(day=28, month=2, year=convdate.year-1))

