# PLEASE TAKE NOTE: 

# UDFs can only support
# return values of 
# type String.

# Function names that 
# start with __ are
# considered private
# functions and will not
# be directly invokable.

def cutFromRight(colName, delim):
    return colName[colName.rfind(delim)+len(delim):]

def fixDate(colName):
    if "ago" in colName:
        return "5 Apr 2017"
    else:
        return colName