import gdb
import gdb.printing

class TupleBuf():
    def __init__(self, tupleBuf, kvMeta):
        self.tupleBuf = tupleBuf
        self.numRows = int(tupleBuf["hdr"]["numTuples"])
        self.validMap = tupleBuf["hdr"]["validMap"]
        self.kvMeta = kvMeta
        self.valuesDesc = kvMeta["valuesDesc"]

    def getType(self, valueTypeStr):
        if (str(valueTypeStr) == "DfInt32"):
            return gdb.lookup_type("int32_t")
        elif (str(valueTypeStr) == "DfUInt32"):
            return gdb.lookup_type("uint32_t")
        elif (str(valueTypeStr) == "DfFloat32"):
            return gdb.lookup_type("float32_t")
        elif (str(valueTypeStr) == "DfFloat64"):
            return gdb.lookup_type("float64_t")
        elif (str(valueTypeStr) == "DfUInt64"):
            return gdb.lookup_type("uint64_t")
        elif(str(valueTypeStr) == "DfInt64"):
            return gdb.lookup_type("int64_t")
        elif(str(valueTypeStr) == "DfFatptr"):
            return gdb.lookup_type("int64_t")
        elif (str(valueTypeStr) == "DfScalarObj"):
            return gdb.lookup_type("Scalar")
        elif (str(valueTypeStr) == "DfScalarPtr"):
            return gdb.lookup_type("Scalar").pointer()
        elif (str(valueTypeStr) == "DfOpRowMetaPtr"):
            return gdb.lookup_type("OpRowMeta").pointer()
        elif (str(valueTypeStr) == "DfString"):
            return gdb.lookup_type("char")
        else:
            raise Exception("Unknown type %s" % valueTypeStr)

    def getValue(self, valueAddr, valueTypeStr):
        valueType = self.getType(valueTypeStr)
        if str(valueTypeStr) == "DfString":
            val = str(gdb.Value(valueAddr).cast(gdb.lookup_type("char").pointer()))
            val = "%d %s" % (len(val) - 17, val[15:])
        else:
            val = gdb.Value(valueAddr).cast(valueType.pointer())
            val = val.dereference()
        return "[%s]" % val

    def isVariableSize(self, valueType):
        valueTypeStr = str(valueType)
        if valueTypeStr == "DfBlob" or valueTypeStr == "DfString" or valueTypeStr == "DfScalarObj" or valueTypeStr == "DfUnknown" or valueTypeStr == "DfMixed":
            return True
        else:
            return False

    def getValueSize(self, valueAddr, valueType):
        if (self.isVariableSize(valueType)):
            if (str(valueType) == "DfScalarObj"):
                scalarObj = gdb.Value(valueAddr).cast(self.getType(valueType).pointer()).dereference()
                return self.getType(valueType).sizeof + int(scalarObj["fieldUsedSize"])
            elif (str(valueType) == "DfUnknown"):
                return 0
            elif (str(valueType) == "DfString"):
                return len(str(gdb.Value(valueAddr).cast(gdb.lookup_type("char").pointer()))) - 16
        else:
            return (self.getType(valueType).sizeof)

        raise Exception("Unknown type %s" % str(valueType))

    def bitmapTestBit(self, validMap, bitNum):
        return (validMap["words"][bitNum / 64] & (1 << (bitNum % 64)) != 0)

    def printBuf(self):
        addr = int(str(self.tupleBuf["buf"]).split(" ")[0], 16)
        numFields = int(self.valuesDesc["numValuesPerTuple"])
        print ("numRows: %d" % self.numRows)
        fieldNames = [ "%s" % self.valuesDesc["valueType"][ii] for ii in range(0, numFields) ]
        print("Schema: [%s]\n" % ", ".join(fieldNames))

        for ii in range(0, self.numRows):
            print("Row %d: {" % ii);
            for jj in range(0, numFields):
                isValid = self.bitmapTestBit(self.validMap, (ii * (numFields)) + jj)
                valueType = self.valuesDesc["valueType"][jj]
                if not isValid:
                    continue

                print("%d  %s: (%s) %s %s" % (jj, valueType, str(self.getType(valueType).pointer()), hex(addr), self.getValue(addr, valueType)));
                valueSize = self.getValueSize(addr, valueType)
                addr += valueSize
            print("}\n")

class TupleBufPrinter(gdb.Command):
    """ TupleBufPrinter
    Prints the contents of a tupleBuf

    (gdb) printTupleBuf <tupleBuf> <kvMeta>
    """

    def __init__ (self):
        super (TupleBufPrinter, self).__init__("printTupleBuf", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        argv = gdb.string_to_argv(arg)
        if (len(argv) != 2):
            print("Usage: printTupleBuf <tupleBuf> <kvMeta>")
            return
        tupleBuf = TupleBuf(gdb.parse_and_eval(argv[0]), gdb.parse_and_eval(argv[1]))

        tupleBuf.printBuf()

TupleBufPrinter()
