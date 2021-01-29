# PLEASE TAKE NOTE: 

# UDFs can only support
# return values of 
# type String.

# Function names that 
# start with __ are
# considered private
# functions and will not
# be directly invokable.

# PLEASE TAKE NOTE: 

# UDFs can only support
# return values of 
# type String.

# Function names that 
# start with __ are
# considered private
# functions and will not
# be directly invokable.

# PLEASE TAKE NOTE: 

# UDFs can only support
# return values of 
# type String.

# Function names that 
# start with __ are
# considered private
# functions and will not
# be directly invokable.

import json
import string
import sys
import random

import sys
from collections import OrderedDict
import string

##Define your own dag
tablesDag = {'test_tab' : [] }

#sortedTables = OrderedDict()

class Counter:
    def __init__(self, low, high):
        self.current = low
        self.high = high

    def __iter__(self):
        return self

    def __next__(self):
        if self.current > self.high:
            raise StopIteration
        else:
            self.current += 1
            return self.current - 1

class Field(object):
    def __init__(self, name, dType, constraintType = None, refTab = None, field = None):
        self.name = name
        self.dType = dType
        self.constraintType = constraintType
        if constraintType == 'p_key':
            self.counter = Counter(1, sys.maxsize)
        self.references = None
        if refTab and field:
            self.references = (refTab, field)
    
    def getColAsDict(self):
        return {"name": self.name, "dType": self.dType, "constraintType": self.constraintType}

class Table(object):

    def __init__(self, name, cols):
        self.name = name
        self.cols = cols

    def getKey(self):
        return self.cols[0]

    @classmethod
    def createTable(cls, name):
        key = Field(name = name + '_key', dType = 'integer', constraintType = 'p_key')

        f_keys = [] ##create foreign keys
        for dep_tab in tablesDag[name]:
            f_tab = sortedTables[dep_tab]
            f_key = Field('f_key_' + str(f_tab), dType = 'integer', constraintType = 'f_key', refTab = f_tab, field = f_tab.getKey())
            f_keys.append(f_key)

        cols = []
        for i in range(1, 300):
            col = Field(name + '_col_' + str(i), dType = 'varchar(20)')
            cols.append(col)

        opcode_col = Field('opcode', dType = 'integer')
        cols.append(opcode_col)

        newTab = cls(name = name, cols = [key] + f_keys + cols)
        return newTab

    def __str__(self):
        return self.name

    def getSchema(self):
        result = []
        for col in self.cols:
            result.append(col.getColAsDict())
        return result

def sortTables():
    sortedTables = OrderedDict()
    try:
        visited_tabs = set()

        def dfs(tab):
            visited_tabs.add(tab)
            for dep in tablesDag[tab]:
                if dep not in visited_tabs:
                    dfs(dep)
            sortedTables[tab] = Table.createTable(tab)

        for tab in tablesDag:
            if tab not in visited_tabs:
                dfs(tab)
        return sortedTables
    except Exception as e:
        print("Error parsing table dependencies!")
        raise e


numRecords = 100000000
class Counter:
    def __init__(self, low, high):
        self.current = low
        self.high = high

    def __iter__(self):
        return self

    def __next__(self):
        if self.current > self.high:
            raise StopIteration
        else:
            self.current += 1
            return self.current - 1
        
def _getVal(field, imd=1, counter=Counter(1, sys.maxsize)):
    global numRecords
        
    constraintType = field["constraintType"]
    if constraintType:
        if constraintType == 'p_key':
            return next(counter)
        elif constraintType == 'f_key':
            return random.randint(1, numRecords)
    if field["name"] == "opcode":
        return imd
    
    if field["dType"] == 'integer':
        return random.randint(1, 10000000)
    elif field["dType"] == 'varchar(20)':
        return ''.join([random.choice(string.ascii_letters) for _ in range(18)])
    
def genData(filepath, instream, startNum=0, imd=False):
    global numRecords
    sortedTables = sortTables()
    schema = sortedTables['test_tab'].getSchema()
    inObj = json.loads(instream.read())
    if inObj["numRows"] == 0:
        return
    start = inObj["startRow"] + 1
    if startNum:
        start += startNum
    end = start + inObj["numRows"]
    colNames = []
    
    numRecords = end
    insertEnd = end
    #for field in schema:
    #    colNames.append(field['name'])
    
    if imd:
        insertEnd = (end + start)//2
        numRecords = insertEnd
    
    counter = Counter(start, end)
    for i in range(start, end):
        res = {}
        imd = 1
        if i > insertEnd:
            imd = 0 if imd > 0 else 2
        for idx, field in enumerate(schema):
            val = _getVal(field, imd=imd, counter=counter)
            res[field["name"]] = val
        yield res