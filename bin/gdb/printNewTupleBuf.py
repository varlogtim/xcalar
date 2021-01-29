import gdb
import gdb.printing


class NewTupleBuf():
    def __init__(self, newTupleBuf, newTupleMeta):
        self.newTupleBuf = newTupleBuf
        self.newTupleMeta = newTupleMeta

    def roundDown(self, val, nearest):
        return ((val) - (val) % (nearest))

    def roundUp(self, val, nearest):
        if (((val) % (nearest)) == 0):
            return val
        else:
            return self.roundDown((val) + (nearest), (nearest))

    def getType(self, fieldTypeStr):
        if (str(fieldTypeStr) == "DfInt32"):
            return gdb.lookup_type("int32_t")
        elif (str(fieldTypeStr) == "DfUInt32"):
            return gdb.lookup_type("uint32_t")
        elif (str(fieldTypeStr) == "DfFloat32"):
            return gdb.lookup_type("float32_t")
        elif (str(fieldTypeStr) == "DfFloat64"):
            return gdb.lookup_type("float64_t")
        elif (str(fieldTypeStr) == "DfUInt64"):
            return gdb.lookup_type("uint64_t")
        elif (str(fieldTypeStr) == "DfInt64"):
            return gdb.lookup_type("int64_t")
        elif (str(fieldTypeStr) == "DfFatptr"):
            return gdb.lookup_type("int64_t")
        elif (str(fieldTypeStr) == "DfScalarObj"):
            return gdb.lookup_type("Scalar")
        elif (str(fieldTypeStr) == "DfScalarPtr"):
            return gdb.lookup_type("Scalar").pointer()
        elif (str(fieldTypeStr) == "DfOpRowMetaPtr"):
            return gdb.lookup_type("OpRowMeta").pointer()
        elif (str(fieldTypeStr) == "DfString"):
            return gdb.lookup_type("char")
        else:
            raise Exception("Unknown type %s" % fieldTypeStr)

    def getValue(self, valueAddr, valueTypeStr, valueLength):
        valueType = self.getType(valueTypeStr)
        if str(valueTypeStr) == "DfString":
            val = str(
                gdb.Value(valueAddr).cast(gdb.lookup_type("char").pointer()))
            val = "%d, 0x%x, '%s'" % (valueLength, valueAddr,
                                      val[16:16 + valueLength])
        else:
            val = gdb.Value(valueAddr).cast(valueType.pointer())
            val = val.dereference()
        return "[%s]" % val

    def isFixedSize(self, fieldType):
        fieldTypeStr = str(fieldType)
        if fieldTypeStr == "DfBlob" or fieldTypeStr == "DfString" or fieldTypeStr == "DfScalarObj" or fieldTypeStr == "DfUnknown" or fieldTypeStr == "DfMixed" or fieldTypeStr == "DfMoney" or fieldTypeStr == "DfTimespec":
            return False
        elif fieldTypeStr == "DfInt32" or fieldTypeStr == "DfUInt32" or fieldTypeStr == "DfInt64" or fieldTypeStr == "DfUInt64" or fieldTypeStr == "DfFloat32" or fieldTypeStr == "DfFloat64" or fieldTypeStr == "DfBoolean" or fieldTypeStr == "DfFatptr" or fieldTypeStr == "DfNull" or fieldTypeStr == "DfOpRowMetaPtr":
            return True
        else:
            raise Exception("Invalid fieldTypeStr %s" % fieldTypeStr)

    def getVariableValueSize(self, valueAddr, fieldType):
        assert (not self.isFixedSize(fieldType))
        if (str(fieldType) == "DfScalarObj"):
            scalarObj = gdb.Value(valueAddr).cast(
                self.getType(fieldType).pointer()).dereference()
            return self.getType(fieldType).sizeof + int(
                scalarObj["fieldUsedSize"])
        elif (str(fieldType) == "DfUnknown"):
            return 0
        elif (str(fieldType) == "DfString"):
            return len(
                str(
                    gdb.Value(valueAddr).cast(
                        gdb.lookup_type("char").pointer()))) - 16

    def getPerRecordBmapSize(self, numFields):
        return self.roundUp(
            gdb.parse_and_eval("NewTupleMeta::BmapNumBitsPerField") *
            numFields, gdb.parse_and_eval(
                "BitsPerUInt8")) >> gdb.parse_and_eval("BitsPerUInt8Shift")

    def getBmapForField(self, recordStartAddr, fieldIdx):
        bmapShft = gdb.parse_and_eval("(%d & (NewTupleMeta::BmapNumEntriesPerByte - 1)) << NewTupleMeta::BmapNumBitsPerFieldShft" % (fieldIdx))
        byteIdx = gdb.parse_and_eval("%d >> NewTupleMeta::BmapNumEntriesPerByteShft" % (fieldIdx))
        curBmapAddr = gdb.parse_and_eval("(uint8_t *) ((uintptr_t) %d + %d)" % (recordStartAddr, byteIdx))
        bmapMask = gdb.parse_and_eval("((uint8_t) NewTupleMeta::BmapMask << %d)" % (bmapShft))
        bmapValue = gdb.parse_and_eval("(((*%d) & %d) >> %d)" % (curBmapAddr, bmapMask, bmapShft))
        return bmapValue

    def isFieldBmapZeroByteInvalid(self, fieldBmap):
        return fieldBmap == gdb.parse_and_eval(
            "NewTupleMeta::BmapInvalid") or fieldBmap == gdb.parse_and_eval(
                "NewTupleMeta::BmapMarkInvalid")

    def isFieldBmapOneByteSize(self, fieldBmap):
        return fieldBmap == gdb.parse_and_eval(
            "NewTupleMeta::BmapOneByteField"
        ) or fieldBmap == gdb.parse_and_eval(
            "NewTupleMeta::BmapOneByteFieldMarkInval")

    def isFieldBmapTwoBytesSize(self, fieldBmap):
        return fieldBmap == gdb.parse_and_eval(
            "NewTupleMeta::BmapTwoBytesField"
        ) or fieldBmap == gdb.parse_and_eval(
            "NewTupleMeta::BmapTwoBytesFieldMarkInval")

    def isFieldBmapThreeBytesSize(self, fieldBmap):
        return fieldBmap == gdb.parse_and_eval(
            "NewTupleMeta::BmapThreeBytesField"
        ) or fieldBmap == gdb.parse_and_eval(
            "NewTupleMeta::BmapThreeBytesFieldMarkInval")

    def isFieldBmapFourBytesSize(self, fieldBmap):
        return fieldBmap == gdb.parse_and_eval(
            "NewTupleMeta::BmapFourBytesField"
        ) or fieldBmap == gdb.parse_and_eval(
            "NewTupleMeta::BmapFourBytesFieldMarkInval")

    def isFieldBmapFiveBytesSize(self, fieldBmap):
        return fieldBmap == gdb.parse_and_eval(
            "NewTupleMeta::BmapFiveBytesField"
        ) or fieldBmap == gdb.parse_and_eval(
            "NewTupleMeta::BmapFiveBytesFieldMarkInval")

    def isFieldBmapSixBytesSize(self, fieldBmap):
        return fieldBmap == gdb.parse_and_eval(
            "NewTupleMeta::BmapSixBytesField"
        ) or fieldBmap == gdb.parse_and_eval(
            "NewTupleMeta::BmapSixBytesFieldMarkInval")

    def isFieldBmapEightBytesSize(self, fieldBmap):
        return fieldBmap == gdb.parse_and_eval(
            "NewTupleMeta::BmapEightBytesField"
        ) or fieldBmap == gdb.parse_and_eval(
            "NewTupleMeta::BmapEightBytesFieldMarkInval")

    def fieldCopyHelper(self, fieldSize, curValueAddr, fieldValue):
        for ii in range(0, fieldSize):
            tmpVal = int(gdb.parse_and_eval("((uint8_t *) (%d))[%d]" % (curValueAddr, ii)))
            fieldValue = (fieldValue | tmpVal << (ii * 8))
        return fieldValue

    def printBuf(self, startRow, printFn):
        numFields = int(self.newTupleMeta["numFields_"])
        fieldTypes = [
            "%d: %s" % (ii, self.newTupleMeta["fieldType_"][ii])
            for ii in range(0, numFields)
        ]
        printFn("NewTupleBuf Schema: [%s]\n" % ", ".join(fieldTypes))
        numTuples = int(self.newTupleBuf["numTuples_"])
        endAddr = int(str(self.newTupleBuf["bufferEnd_"]).split(" ")[0], 16)
        bufSize = int(self.newTupleBuf["bufSize_"])
        bufUsedFromBegin = int(self.newTupleBuf["bufUsedFromBegin_"])
        bufUsedFromEnd = int(self.newTupleBuf["bufUsedFromEnd_"])
        bufSizeTotal = int(self.newTupleBuf["bufSizeTotal_"])
        bufRemaining = int(self.newTupleBuf["bufRemaining_"])
        startAddr = int(
            str(self.newTupleBuf["bufferStart_"]).split(" ")[0], 16)
        printFn(
            "NewTupleBuf Header: [numTuples %d, endAddr 0x%x, bufSize %d, bufUsedFromBegin %d, bufUsedFromEnd %d, bufSizeTotal %d, bufRemaining %d, startAddr 0x%x]\n"
            % (numTuples, endAddr, bufSize, bufUsedFromBegin, bufUsedFromEnd,
               bufSizeTotal, bufRemaining, startAddr))

        bytesFromStart = 0
        bytesFromEnd = 0
        for tupleIter in range(0, numTuples):
            nextTuple = startAddr + bytesFromStart
            curFromStart = self.getPerRecordBmapSize(numFields)
            curFromEnd = 0

            if tupleIter >= startRow:
                printFn("Row %d: {\n" % tupleIter)
            for ii in range(0, numFields):
                fieldBmapValue = self.getBmapForField(nextTuple, ii)
                fieldType = self.newTupleMeta["fieldType_"][ii]
                curFieldSize = gdb.parse_and_eval(
                    "NewTupleMeta::BmapValueValidFieldSizeLUT[%d]" % fieldBmapValue)
                curVarFieldSize = 0
                curValStartAddr = nextTuple + curFromStart
                curValEndAddr = 0

                if self.isFixedSize(fieldType) or str(
                        fieldType) == "DfUnknown":
                    formatStr = "[%d] %s (%d): %s\t%u"
                    fieldValue = 0
                    if self.isFieldBmapZeroByteInvalid(fieldBmapValue):
                        formatStr = "[%d] %s: %s\t%s"
                        fieldValue = "FNF"
                        if fieldBmapValue == gdb.parse_and_eval(
                                "NewTupleMeta::BmapMarkInvalid"):
                            formatStr += " (Invalid)"
                        if tupleIter >= startRow:
                            formatStr += "\n"
                            printFn(
                                formatStr % (ii, fieldType,
                                             hex(int(curValStartAddr)), fieldValue))
                        continue
                    elif self.isFieldBmapOneByteSize(fieldBmapValue):
                        fieldValue = self.fieldCopyHelper(curFieldSize, curValStartAddr, fieldValue)
                        if fieldBmapValue == gdb.parse_and_eval(
                                "NewTupleMeta::BmapOneByteFieldMarkInval"):
                            formatStr += " (Invalid)"
                    elif self.isFieldBmapTwoBytesSize(fieldBmapValue):
                        fieldValue = self.fieldCopyHelper(curFieldSize, curValStartAddr, fieldValue)
                        if fieldBmapValue == gdb.parse_and_eval(
                                "NewTupleMeta::BmapTwoBytesFieldMarkInval"):
                            formatStr += " (Invalid)"
                    elif self.isFieldBmapThreeBytesSize(fieldBmapValue):
                        fieldValue = self.fieldCopyHelper(curFieldSize, curValStartAddr, fieldValue)
                        if fieldBmapValue == gdb.parse_and_eval(
                                "NewTupleMeta::BmapThreeBytesFieldMarkInval"):
                            formatStr += " (Invalid)"
                    elif self.isFieldBmapFourBytesSize(fieldBmapValue):
                        fieldValue = self.fieldCopyHelper(curFieldSize, curValStartAddr, fieldValue)
                        if fieldBmapValue == gdb.parse_and_eval(
                                "NewTupleMeta::BmapFourBytesFieldMarkInval"):
                            formatStr += " (Invalid)"
                    elif self.isFieldBmapFiveBytesSize(fieldBmapValue):
                        fieldValue = self.fieldCopyHelper(curFieldSize, curValStartAddr, fieldValue)
                        if fieldBmapValue == gdb.parse_and_eval(
                                "NewTupleMeta::BmapFiveBytesFieldMarkInval"):
                            formatStr += " (Invalid)"
                    elif self.isFieldBmapSixBytesSize(fieldBmapValue):
                        fieldValue = self.fieldCopyHelper(curFieldSize, curValStartAddr, fieldValue)
                        if fieldBmapValue == gdb.parse_and_eval(
                                "NewTupleMeta::BmapSixBytesFieldMarkInval"):
                            formatStr += " (Invalid)"
                    else:
                        assert self.isFieldBmapEightBytesSize(fieldBmapValue)
                        fieldValue = self.fieldCopyHelper(curFieldSize, curValStartAddr, fieldValue)
                        if fieldBmapValue == gdb.parse_and_eval(
                                "NewTupleMeta::BmapEightBytesFieldMarkInval"):
                            formatStr += " (Invalid)"
                    if tupleIter >= startRow:
                        formatStr += "\n"
                        printFn(formatStr % (ii, fieldType, curFieldSize,
                                             hex(int(curValStartAddr)), fieldValue))
                else:
                    formatStr = "[%d] %s (%d): %s\t%s"
                    if self.isFieldBmapZeroByteInvalid(fieldBmapValue):
                        formatStr = "[%d] %s : %s\t%s"
                        fieldValue = "FNF"
                        if fieldBmapValue == gdb.parse_and_eval(
                                "NewTupleMeta::BmapMarkInvalid"):
                            formatStr += " (Invalid)"
                        if tupleIter >= startRow:
                            formatStr += "\n"
                            printFn(
                                formatStr % (ii, fieldType,
                                             hex(int(curValStartAddr)), fieldValue))
                        continue
                    elif self.isFieldBmapOneByteSize(fieldBmapValue):
                        if fieldBmapValue == gdb.parse_and_eval(
                                "NewTupleMeta::BmapOneByteFieldMarkInval"):
                            formatStr += " (Invalid)"
                        curVarFieldSize = self.fieldCopyHelper(curFieldSize, curValStartAddr, curVarFieldSize)
                        curValEndAddr = endAddr - bytesFromEnd - curFromEnd - curVarFieldSize
                    elif self.isFieldBmapTwoBytesSize(fieldBmapValue):
                        if fieldBmapValue == gdb.parse_and_eval(
                                "NewTupleMeta::BmapTwoByteFieldMarkInval"):
                            formatStr += " (Invalid)"
                        curVarFieldSize = self.fieldCopyHelper(curFieldSize, curValStartAddr, curVarFieldSize)
                        curValEndAddr = endAddr - bytesFromEnd - curFromEnd - curVarFieldSize
                    elif self.isFieldBmapThreeBytesSize(fieldBmapValue):
                        if fieldBmapValue == gdb.parse_and_eval(
                                "NewTupleMeta::BmapThreeByteFieldMarkInval"):
                            formatStr += " (Invalid)"
                        curVarFieldSize = self.fieldCopyHelper(curFieldSize, curValStartAddr, curVarFieldSize)
                        curValEndAddr = endAddr - bytesFromEnd - curFromEnd - curVarFieldSize
                    else:
                        assert self.isFieldBmapFourBytesSize(fieldBmapValue)
                        if fieldBmapValue == gdb.parse_and_eval(
                                "NewTupleMeta::BmapFourBytesFieldMarkInval"):
                            formatStr += " (Invalid)"
                        curVarFieldSize = self.fieldCopyHelper(curFieldSize, curValStartAddr, curVarFieldSize)
                        curValEndAddr = endAddr - bytesFromEnd - curFromEnd - curVarFieldSize
                    if tupleIter >= startRow:
                        formatStr += "\n"
                        printFn(
                            formatStr % (ii, fieldType, curFieldSize,
                                         hex(int(curValStartAddr)),
                                         self.getValue(curValEndAddr, fieldType,
                                                       curVarFieldSize)))

                # Seek to next field
                curFromStart = curFromStart + curFieldSize
                curFromEnd = curFromEnd + curVarFieldSize

            # Seek to next tuple
            bytesFromStart = bytesFromStart + curFromStart
            bytesFromEnd = bytesFromEnd + curFromEnd
            if tupleIter >= startRow:
                printFn("}\n")


class NewTupleBufPrinter(gdb.Command):
    """ NewTupleBufPrinter
    Prints the contents of a NewTuplesBuffer which can be fixed or variable fields type.

    (gdb) printNewTupleBuf <NewTuplesBuffer> <NewTupleMeta>
    """

    def __init__(self):
        super(NewTupleBufPrinter, self).__init__("printNewTupleBuf",
                                                 gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        argv = gdb.string_to_argv(arg)
        if (len(argv) < 2):
            print(
                "Usage: printNewTupleBuf <NewTuplesBuffer> <NewTupleMeta> <startRow=0>"
            )
            return
        newTuplesBuffer = gdb.parse_and_eval(argv[0])
        newTupleMeta = gdb.parse_and_eval(argv[1])
        if len(argv) > 2:
            startRow = gdb.parse_and_eval(argv[2])
        else:
            startRow = 0

        print("\nGiven NewTuplesBuffer: %s, NewKeyValueMeta: %s StartRow: %s\n"
              % (newTuplesBuffer, newTupleMeta, startRow))
        print("================================")
        newTupleBuf = NewTupleBuf(newTuplesBuffer, newTupleMeta)
        newTupleBuf.printBuf(startRow, sys.stdout.write)
        print("================================")


class ExportXdb(gdb.Command):
    """ exportXdb

    (gdb) exportXdb fileName xdbAddress

    Example:
    (gdb) exportXdb tmp.txt 0x7f3d7c5f7be8
    """

    def __init__(self):
        super(ExportXdb, self).__init__("exportXdb", gdb.COMMAND_DATA)

    def getRawAddr(self, addr):
        return addr.cast(gdb.lookup_type('unsigned long'))

    def getPageAddr(self, xdbPageUseInlinesToModify):
        return xdbPageUseInlinesToModify & 0xffffffffffffffc

    def dumpXdb(self, fileName, xdb):
        xdbMeta = xdb["meta"]
        kvNamedMeta = xdbMeta["kvNamedMeta"]
        newTupleMeta = kvNamedMeta["kvMeta_"]["tupMeta_"]
        numFields = int(newTupleMeta["numFields_"])
        colNames = kvNamedMeta["valueNames_"]

        hashSlotInfo = xdb["hashSlotInfo"]
        numSlots = int(hashSlotInfo["hashSlots"])

        with open("/tmp/{}".format(fileName), "w") as fp:
            fp.write("Column Headers: {}\n".format(",".join(
                [str(colNames[ii]).split(",")[0] for ii in range(numFields)])))
            for ii in range(0, numSlots):
                fp.write("======= Slot %d ======\n" % ii)
                nextPageUseInlinesToModify = hashSlotInfo["hashBase"][ii][
                    'nextPageUseInlinesToModify']
                xdbPage = self.getPageAddr(
                    self.getRawAddr(nextPageUseInlinesToModify))
                while xdbPage:
                    xdbPageStruct = gdb.parse_and_eval('(struct XdbPage *)' +
                                                       str(xdbPage))
                    fp.write('Page 0x%x: %s\n' %
                             (xdbPage, str(xdbPageStruct.dereference())))
                    xdbPage = self.getRawAddr(xdbPageStruct['hdr']['nextPage'])

                    tupleBuf = xdbPageStruct["tupBuf"]
                    tupBufPrinter = NewTupleBuf(tupleBuf, newTupleMeta)
                    tupBufPrinter.printBuf(0, fp.write)
                    fp.write("\n")

                fp.write("=======================\n")

    def invoke(self, arg, from_tty):
        try:
            argv = gdb.string_to_argv(arg)
            self.dumpXdb(argv[0], gdb.parse_and_eval('(Xdb *)' + argv[1]))
        except Exception as e:
            print(str(e))
            traceback.print_exc()


NewTupleBufPrinter()
ExportXdb()
