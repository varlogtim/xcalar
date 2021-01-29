# PLEASE TAKE NOTE: 

# UDFs can only support
# return values of 
# type String.

# Function names that 
# start with __ are
# considered private
# functions and will not
# be directly invokable.

def fixed_to_dictnotfouond(inPath, inStream):
    # The following field names and offsets are taken from section 4 (Daily Quotes File Data
    # Fields) of the Daily TAQ Client Specification (version 2.1)
    # https://www.nyxdata.com/doc/243156
    #result = "Time,Exchange,Symbol,Bid Price,Bid Size,Ask Price,Ask Size,Quote Condition,Market Maker,Bid Exchange,Ask Exchange,Sequence Number,National BBO Ind,NASDAQ BBO Ind,Quote Cancel/Correction,Source of Quote,Retail Interest Indicator,Short Sale Restriction Indicator,LULD BBO Indicator (CQS),LLULD BBO Indicator (UTP), FINRA ADF MPID Indicator,Sip-generated Message Identifier,National BBO LULD Indicator,Participant Timestamp,Regional Reference Number,Trade Reporting Facility"
    result = "Time,Exchange,Symbol,BidPrice,BidSize,AskPrice,AskSize,QuoteCondition,MarketMaker,BidExchange,AskExchange,SequenceNumber,NationalBBOInd,NASDAQBBOInd,QuoteCancel_Correction,SourceofQuote,RetailInterestIndicator,ShortSaleRestrictionIndicator,LULDBBOIndicator_CQS,LLULDBBOIndicator_UTP,FINRAADFMPIDIndicator,Sip-generatedMessageIdentifier,NationalBBOLULDIndicator,ParticipantTimestamp,RegionalReferenceNumber,TradeReportingFacility"
    field_indices = [(0,12),(12,13),(13,29),(29,40),(40,47),(47,58),(58,65),(65,66),(66,70),(70,71),(71,72),(72,88),(88,89),(89,90),(90,91),(91,92),(92,93),(93,94),(94,95),(95,96),(96,97),(97,98),(98,99),(99,111),(111,119),(119,131)]

    colHeaders = result.split(",")
    first = True
    for ln in inStream.readlines():
        ln = ln.decode("utf-8")
        if first:
            first = False
            # First line contains date and record count, ignore for now.  Consider that
            # original file may have been split into parts with only the first part having
            # the record count.
            if "Record" in ln:
                continue
        # handle EOF
        if len(ln) < 2:
            continue
        field = [ln[x:y] for (x,y) in field_indices]

        # build a dict from the column names and fields
        record = {}
        c = 0
        for f in field:
            record[colHeaders[c]] = f
            # print("header {} contains {}".format(colHeaders[c], f))
            c += 1

        yield record
