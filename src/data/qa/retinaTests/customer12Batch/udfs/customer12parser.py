# PLEASE TAKE NOTE: 

# UDFs can only support
# return values of 
# type String.

# Function names that 
# start with __ are
# considered private
# functions and will not
# be directly invokable.

# Streaming UDF for parsing text-based customer12 feed.
#
# Emits one record per tweet, per ticker symbol found in the tweet text.
# So if there are two ticker symbols found in a tweet, there will be two
# records emitted for that tweet, with the symbol found in the "tweetFound"
# field.  If no ticker symbols are found, emits exactly one record per tweet.


import sys
from codecs import getreader
Utf8Reader = getreader("utf-8")

def streamCustomer12(fullPath, inStream):

    # List of tickers to search for.
    tickerList = ["AAPL","AMZN","BAC","BRA","BRKA","CVX","FB","GE","GOOG", \
                  "JNJ","JPM","MSFT","ORCL","PFE","PG","T","V","VZ","WFC", \
                  "WMT","XOM"]

    utf8Stream = Utf8Reader(inStream)
    line = utf8Stream.readline()
    while line:
        # Look for a line starting with "Like" to indicate the beginning
        # of the next tweet.
        while line[0:4] != "Like":
            #print line
            line = utf8Stream.readline()
            # XXX clean up this "if not" business throughout
            if not line:
                break
        tweetDict = {}
        tweetDict["sourceFile"] = fullPath
        line = utf8Stream.readline()
        # "Promoted" is an optional line in each tweet
        if line[1:9] == "Promoted":
            tweetDict["Promoted"] = True
            tweetDict["tweeter"] = utf8Stream.readline()[0:-1]
        # "follows" is also optional, e.g., " Barack Obama follows"
        elif (line[-8:-1] == "follows") or (line[-7:-1] == "follow"):
            tweetDict["follows"] = line[1:-8]
            tweetDict["tweeter"] = utf8Stream.readline()[0:-1]
        else:
            tweetDict["Promoted"] = False
            tweetDict["tweeter"] = line[0:-1]
        # We expect a "More" string here.  Just skip over it.
        more = utf8Stream.readline()
        if not more:
            break
        line = utf8Stream.readline()
        if not line:
            break
        # "Replying to" is an optional line in each tweet
        if line[0:12] == "Replying to ":
            tweetDict["replyTo"] = line[12:-1]
            tweetDict["tweetText"] = utf8Stream.readline()[0:-1]
        else:
            tweetDict["tweetText"] = line[0:-1]
        line = utf8Stream.readline()
        if not line:
            break
        # There may be multiple lines in the tweet text.  Iterate
        # lines until we find the line with "replies" in it, which
        # we refer to as the "tweetStatus".
        while line and ("replies" not in line) and ("reply" not in line):
            tweetDict["tweetText"] = tweetDict["tweetText"] + " " + line[0:-1]
            line = utf8Stream.readline()
        tweetDict["tweetStatus"] = line[0:-1]

        # We now have our tweet in tweetDict.  Now look for stock symbols
        # and emit one copy of the tweet record per stock symbol detected.
        # If no symbols are detected, emit one record with stock symbol
        # "none found".
        oneFound = False
        for ticker in tickerList:
            if (ticker + " ") in tweetDict["tweetText"] or \
            (ticker + ":") in tweetDict["tweetText"]:
                tweetDict["tickerFound"] = ticker
                oneFound = True
                yield tweetDict

        if not oneFound:
            tweetDict["tickerFound"] = "none found"
            yield tweetDict

        line = utf8Stream.readline()


if __name__ == '__main__':
    testfile = str(sys.argv[1])
    f = open(testfile, "r")
    outDict = streamCustomer12("/this/path", f)
    for myDict in outDict:
        print(myDict)