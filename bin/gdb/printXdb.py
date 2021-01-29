import gdb
import gdb.printing
import gdb.types
from gdb import Type
import traceback
import re
import argparse
from collections import Counter
import json

XdbTypeName = "struct Xdb"
TupleBufTypeName = "struct XdbPage<NewTuplesBuffer<VarNewTuplesBuf>>"

class GetXdb(gdb.Command):
    """ GetXdb

    (gdb) getXdb slowHashTable xdbId

    Example:
    (gdb) p xdbId
    $65 = 72057594038035305
    (gdb) getXdb slowHashTable 72057594038035305
    print sizeof(slowHashTable.slots)/sizeof(slowHashTable.slots[0])
    hook: 0x7f3d7c5f7c00, offset: 24
    numSlots: 32749 slowHashTable.slots[7033]
      Xdb at 0x7f3d7c5f7be8 -- xdbId: 72057594038035305

    """
    def __init__(self):
        super(GetXdb, self).__init__("getXdb", gdb.COMMAND_DATA)

    def invoke(self, arg, from_tty):

        XdbType = gdb.lookup_type(XdbTypeName)
        argv = gdb.string_to_argv(arg)
        if (len(argv) < 2):
            print("getXdb slowHashTable <xdbId>\n")
            return

        slowHashTable = gdb.parse_and_eval(argv[0])
        print ("print sizeof(%s.slots)/sizeof(%s.slots[0])" % (argv[0], argv[0]))
        numSlots = int(gdb.parse_and_eval("sizeof(%s.slots)/sizeof(%s.slots[0])" % (argv[0], argv[0])))
        xdbId = int(argv[1])
        slotIdx = xdbId % numSlots
        hook = slowHashTable["slots"][slotIdx]
        xdbId = None
        found = False
        offset = int(str(gdb.parse_and_eval("&((%s *) 0)->hook" % XdbTypeName)), 0)
        print("hook: %s, offset: %d" % (hook, offset))
        print("numSlots: %d slowHashTable.slots[%d]" % (numSlots, slotIdx))
        while (int(str(hook), 0) != 0):
            xdbAddr = int(str(hook), 16) - offset
            xdb = gdb.Value(xdbAddr).cast(XdbType.pointer())
            xdbId = int(xdb["xdbId"])
            print ("  Xdb at ((Xdb *)%s) -- xdbId: %d\n" % (hex(xdbAddr), xdbId))
            if (xdbId == xdbId):
                found = True
                break
            hook = hook["next"]

        if not found:
            print("Xdb %d not found\n" % xdbId)

class XdbPrinter(gdb.Command):
    """ XdbPrinter

    (gdb) printXdb xdbAddress [doFullDump]

    Example:
    (gdb) printXdb 0x7f3d7c5f7be8 True
    """
    def __init__(self):
        super(XdbPrinter, self).__init__("printXdb", gdb.COMMAND_DATA)

    def getRawAddr(self, addr):
        return addr.cast(gdb.lookup_type('unsigned long'))

    def getPageAddr(self, xdbPageUseInlinesToModify):
        return xdbPageUseInlinesToModify & 0xffffffffffffffc

    def to_string(self, arg):
        argv = gdb.string_to_argv(arg)
        if (len(argv) < 1):
            print("printXdb xdbAddress [doFullDump]\n")
            return
        xdb = gdb.parse_and_eval('(struct Xdb *)' + argv[0])
        try:
            fullDump = eval(argv[1])
        except:
            fullDump = False
        xdbId = int(xdb["xdbId"])
        hashSlotInfo = xdb["hashSlotInfo"]
        numSlots = int(hashSlotInfo["hashSlots"])
        sumSlotRows = 0
        sumSlotPages = 0
        outputStr = ''
        for ii in range (0, numSlots):
            slotRows = int(hashSlotInfo["hashBaseAug"][ii]["numRows"])
            slotPages = int(hashSlotInfo["hashBaseAug"][ii]["numPages"])
            sumSlotRows += slotRows
            sumSlotPages += slotPages
            nextPageUseInlinesToModify = hashSlotInfo["hashBase"][ii]['nextPageUseInlinesToModify']
            xdbPage = self.getPageAddr(self.getRawAddr(nextPageUseInlinesToModify))
            if fullDump and xdbPage:
                outputStr += '\nSlot %d (%d,%d): ' % (ii, slotRows, slotPages) + str(hashSlotInfo["hashBase"][ii]) + '\n'
                while xdbPage:
                    xdbPageStruct = gdb.parse_and_eval('(struct XdbPage *)' + str(xdbPage))
                    outputStr += 'Page 0x%x: ' % (xdbPage,) + str(xdbPageStruct.dereference()) + '\n'
                    xdbPage = self.getRawAddr(xdbPageStruct['hdr']['nextPage'])
        numRows = int(xdb["numRows"])
        numPages = int(xdb["numPages"])
        outputStr += "\n\nXdb (%d)\n" % xdbId
        outputStr += "  numSlots: %d\n" % numSlots
        outputStr += "  numRows: %d\n" % numRows
        outputStr += "  sum(hashSlotInfo.hashBaseAug[ii].numRows): %d\n" % sumSlotRows
        outputStr += "  numPages: %d\n" % numPages
        outputStr += "  sum(hashSlotInfo.hashBaseAug[ii].numPages): %d\n" % sumSlotPages

        return outputStr

    def invoke(self, arg, from_tty):
        try:
            print("%s" % self.to_string(arg))
        except Exception as e:
            print(str(e))
            traceback.print_exc()

class DiffXdb(gdb.Command):
    """ DiffXdb

    (gdb) diffXdb xdbAddress xdbAddress

    Example:
    (gdb) diffXdb 0x7f3d7c5f7be8 0x7f3d7c5f7be8
    """
    def __init__(self):
        super(DiffXdb, self).__init__("diffXdb", gdb.COMMAND_DATA)

    def getRawAddr(self, addr):
        return addr.cast(gdb.lookup_type('unsigned long'))

    def getPageAddr(self, xdbPageUseInlinesToModify):
        return xdbPageUseInlinesToModify & 0xffffffffffffffc

    def diff(self, arg):
        argv = gdb.string_to_argv(arg)
        if (len(argv) < 2):
            print("diffXdb xdbAddress xdbAddress\n")
            return

        xdb1 = gdb.parse_and_eval('(Xdb *)' + argv[0])
        xdb2 = gdb.parse_and_eval('(Xdb *)' + argv[1])

        hashSlotInfo1 = xdb1["hashSlotInfo"]
        hashSlotInfo2 = xdb2["hashSlotInfo"]

        numSlots = int(hashSlotInfo1["hashSlots"])
        outputStr = ''
        for ii in range (0, numSlots):
            slotRows1 = int(hashSlotInfo1["hashBaseAug"][ii]["numRows"])
            slotRows2 = int(hashSlotInfo2["hashBaseAug"][ii]["numRows"])

            if slotRows1 != slotRows2:
                outputStr += 'Slot %d differs: %d != %d \n' % (ii, slotRows1, slotRows2)

        return outputStr

    def invoke(self, arg, from_tty):
        try:
            print("%s" % self.diff(arg))
        except Exception as e:
            print(str(e))
            traceback.print_exc()

class XdbPageTrace(gdb.Command):
    """ XdbPageTrace

    # Command help
    (gdb) xdbpage-trace -h

    REQUIRES:
    Constants.CtxTracesMode=1 # For all refcount and XDB page tracing
    or
    Constants.CtxTracesMode=2 # As above but also with inflight tracking

    EXAMPLES:
    # Pay close attention to subtle differences in the comments
    #
    # Show all contexts and counts for ALL XDB page refcount incs/decs for ALL time
    (gdb) xdbpage-trace

    # Show all contexts and counts for INFLIGHT XDB page refcount incs/decs
    (gdb) xdbpage-trace -i

    # Show all contexts and counts for ALL XDB page allocs/frees for ALL time
    (gdb) xdbpage-trace -m

    # Show all contexts and counts for INFLIGHT XDB page refcount incs/decs from trace file
    (gdb) xdbpage-trace -imf /tmp/traces.out

    # Show all contexts and counts ONLY for XDB page 0x14d1f438c9b0 refcounts
    (gdb) xdbpage-trace -ia 0x14d1f438c9b0

    # Show the chronologically last n refcount incs/decs
    xdbpage-trace -l

    # Show the chronologically last n XDB page allocs/frees
    xdbpage-trace -lm
    """
    labels = {True: ['allocs', 'frees'], False: ['incs', 'decs']}
    def __init__(self):
        self.data = {}
        super(XdbPageTrace, self).__init__("xdbpage-trace", gdb.COMMAND_DATA)

    def getTraceData(self, elem):
        traceDataFile = {'globalTraces_': self.data,
                         'MaxDbgRefCount': self.data.get('MaxDbgRefCount'),
                         'MaxDbgRefCtxCount': self.data.get('MaxDbgRefCtxCount'),
                         'XcalarConfig::instance->ctxTracesMode_': self.data.get('XcalarConfig::instance->ctxTracesMode_')}
        traceDataMem = {'globalTraces_': gdb.parse_and_eval("globalTraces_"),
                        'MaxDbgRefCount': gdb.parse_and_eval('MaxDbgRefCount'),
                        'MaxDbgRefCtxCount': gdb.parse_and_eval('MaxDbgRefCtxCount'),
                        'XcalarConfig::instance->ctxTracesMode_': gdb.parse_and_eval('XcalarConfig::instance->ctxTracesMode_')}
        traceData = {True: traceDataFile, False: traceDataMem}
        return traceData[bool(self.args.file)][elem]

    def getTraceInflight(self, traces, isInc, maxCtx):
        if isInc:
            itemName = 'incs'
        else:
            itemName = 'decs'

        traceFreqCt = {i: 0 for i in range(0, maxCtx)}
        if self.args.file:
            if self.args.mem:
                numInFlight = len(self.data['tracesMemInFlight'])
                for item in self.data['tracesMemInFlight']:
                    traceFreqCt[int(item['val'])] += 1
            else:
                numInFlight = len(self.data['tracesRefInFlight'])
                for item in self.data['tracesRefInFlight']:
                    for i in range(maxCtx):
                        numRefs = item[itemName][i]
                        if numRefs:
                            traceFreqCt[i] += numRefs
        else:
            if self.args.mem:
                hashTableName = '*globalTraces_->tracesMemInFlight'
            else:
                hashTableName = '*globalTraces_->tracesRefInFlight'
            ht = gdb.parse_and_eval(hashTableName)
            htType = gdb.types.get_basic_type(ht.type)
            valueType = htType.template_argument(1)
            hookMember = htType.template_argument(2)
            slotCount = htType.template_argument(4)
            hookOffset = int(valueType[str(hookMember).split("::")[-1]].bitpos / 8)
            slot = 0
            numInFlight = 0
            while slot < slotCount:
                current = gdb.parse_and_eval("(%s).slots[%d]" % (hashTableName, slot))
                while current:
                    s = "*('%s' *)(((uint8_t *)0x%lx) - 0x%lx)" % (valueType, current, hookOffset)
                    unused = "%lx"%current # Bizarre noop to make this work, see https://bugs.python.org/issue28023
                    item = gdb.parse_and_eval(s)
                    if self.args.addr and str(item['keyPtr']) != self.args.addr:
                        current = current["next"]
                        continue

                    if self.args.mem:
                        traceFreqCt[int(item['val'])] += 1
                    else:
                        for i in range(maxCtx):
                            numRefs = item[itemName][i]
                            if numRefs:
                                traceFreqCt[i] += numRefs

                    numInFlight += 1
                    current = current["next"]

                slot += 1

        numCtx = sum([bool(x) for x in traceFreqCt.values()])
        traceList = []
        for k in sorted(traceFreqCt, key=traceFreqCt.get, reverse=True):
            trace = traces[k]['trace']
            traceSize = traces[k]['traceSize']
            if self.args.file:
                traceStrList = [int(trace[i]) for i in range(0, traceSize)]
            else:
                traceStrList = [int(trace[i].cast(gdb.lookup_type('size_t'))) for i in range(0, traceSize)]
            traceList.append((traceFreqCt[k], traceStrList))

        return (traceList, numCtx, numInFlight)

    def checkOverflow(self, traceName, traces):
        if traces['numRefCtxOverflow']:
            print('='*80)
            print("ERROR: Too many {} contexts, {} lost: RESULTS WILL BE UNRELIABLE."
                    .format(traceName, traces['numRefCtxOverflow']))
            print("ERROR: Increase SaveTrace.cpp::MaxDbgRefCtxCount, recompile and rerun XCE")
            print("ERROR: Pass '-F' flag to this script to force dump the unreliable traces.")
            print('='*80)
            if not self.args.force:
                raise RuntimeError("Too many contexts, increase SaveTrace.cpp::MaxDbgRefCtxCount")

        if traces['numRefCtxElmOverflow']:
            print('='*80)
            print("ERROR: Too many {} element contexts, {} lost: RESULTS WILL BE UNRELIABLE."
                    .format(traceName, traces['numRefCtxElmOverflow']))
            print("ERROR: Increase SaveTrace.cpp::ctxIdx_t type width, recompile and rerun XCE")
            print("ERROR: Pass '-F' flag to this script to force dump the unreliable traces.")
            print('='*80)
            if not self.args.force:
                raise RuntimeError("Too many contexts, increase SaveTrace.cpp::ctxIdx_t type width")


    def getTrace(self, isInc):
        retList = []

        if isInc:
            if self.args.mem:
                traceName = 'tracesAllocs'
            else:
                traceName = 'tracesInc'
        else:
            if self.args.mem:
                traceName = 'tracesFrees'
            else:
                traceName = 'tracesDec'

        if self.args.last:
            numTraces = self.getTraceData('MaxDbgRefCount')
            traceSrc = 'lastRefs'
        else:
            numTraces = self.getTraceData('MaxDbgRefCtxCount')
            traceSrc = 'refCtx'

        globalTraces = self.getTraceData('globalTraces_')
        traces = globalTraces[traceName]
        self.checkOverflow(traceName, traces)
        traceList = []
        numCtx = 0
        numInFlight = 0
        if self.args.inflight:
            (traceList, numCtx, numInFlight) = self.getTraceInflight(traces[traceSrc], isInc, numTraces)
        else:
            for i in range(numTraces):
                trace = traces[traceSrc][i]['trace']
                traceSize = traces[traceSrc][i]['traceSize']
                count = traces[traceSrc][i]['count']

                # Deal with gdb.Value weirdness (ie not iterable)
                if self.args.file:
                    traceStrList = [int(trace[i]) for i in range(0, traceSize)]
                else:
                    traceStrList = [int(trace[i].cast(gdb.lookup_type('size_t'))) for i in range(0, traceSize)]
                traceList.append((count, traceStrList))
                if count > 0:
                    numCtx += 1

        retList = sorted(traceList, key=lambda x: int(x[0]), reverse=True)
        return (retList, numCtx, numInFlight)

    def printTrace(self, incTracesList, decTracesList):
        def addrToStr(addr):
            outStr = gdb.execute("info symbol {}".format(addr), False, True)
            outStr = re.sub(r' in section .*|\(.*\)', r'', outStr)
            outStr = '0x{:0x} {}'.format(int(addr), outStr)
            return outStr[:-1]

        assert(len(incTracesList) == len(decTracesList))
        incTotal = 0
        decTotal = 0
        for (incTrace, decTrace) in zip(incTracesList, decTracesList):
            maxElms = max(len(incTrace[1]), len(decTrace[1]))
            if maxElms == 0:
                continue

            if not incTrace[0] and not decTrace[0] and not self.args.last:
                break

            if self.args.mem:
                incName = 'allocs'
                decName = 'frees '
            else:
                incName = ' incs '
                decName = ' decs '

            fmtStr = "{:<80.78}|   {:<80.78}"
            if self.args.last:
                incHdr = "=========== {} ===========".format(incName)
                decHdr = "=========== {} ===========".format(decName)
            else:
                incHdr = "======== {:^7} {} ========".format(incTrace[0], incName)
                decHdr = "======== {:^7} {} ========".format(decTrace[0], decName)
                incTotal += incTrace[0]
                decTotal += decTrace[0]
            print(fmtStr.format(incHdr, decHdr))
            for i in range(0, maxElms):
                incTraceElm = ''
                decTraceElm = ''
                try:
                    if incTrace[0] or self.args.last:
                        incTraceElm = addrToStr(incTrace[1][i])
                except:
                    pass
                try:
                    if decTrace[0] or self.args.last:
                        decTraceElm = addrToStr(decTrace[1][i])
                except:
                    pass
                print(fmtStr.format(incTraceElm, decTraceElm))
        print("{} {} and {} {}, {} in flight".format(incTotal, self.labels[self.args.mem][0], decTotal, self.labels[self.args.mem][1], incTotal - decTotal))

    def pageTrace(self):
        if self.args.file:
            with open(self.args.file) as fh:
                self.data = json.load(fh)

        ctxTracesMode = self.getTraceData("XcalarConfig::instance->ctxTracesMode_")

        if not ctxTracesMode :
            print('ERROR: Context tracing disabled')
            print('Please add "Constants.CtxTracesMode=14" to your XCE config and rerun')
            return

        if ctxTracesMode < 2 and self.args.inflight:
            print('ERROR: Inflight context tracing disabled')
            print('Please change "Constants.CtxTracesMode=14" in your XCE config and rerun')
            return

        if self.args.addr and not self.args.inflight:
            print('XDB page address only supported for in-flight traces')
            return

        if self.args.last and self.args.inflight:
            print('Last traces option not compatible with inflight option')
            return

        (incTracesList, numIncCtx, numInFlight) = self.getTrace(True)
        if self.args.mem and self.args.inflight:
            decTracesList = [(0, []) for x in range(len(incTracesList))]
            numDecCtx = 0
            self.printTrace(incTracesList, decTracesList)
            print("{} {} contexts (in-flight memory only so no frees)".format(numIncCtx, self.labels[self.args.mem][0]))
        else:
            (decTracesList, numDecCtx, notUsed) = self.getTrace(False)
            self.printTrace(incTracesList, decTracesList)
            print("{} {} contexts and {} {} contexts".format(numIncCtx, self.labels[self.args.mem][0], numDecCtx, self.labels[self.args.mem][1]))

        if numInFlight:
            print("{} pages in flight".format(numInFlight))

    def invoke(self, arg, from_tty):
        try:
            parser = argparse.ArgumentParser(description='XDB debugging')
            parser.add_argument('-a', dest='addr', required=False, type=str,
                                default=None,
                                help='Filter only this XDB page address')
            parser.add_argument('-f', dest='file', required=False, type=str,
                                default=None,
                                help='File to load data from')
            parser.add_argument('-F', dest='force', required=False,
                                default=False, action='store_true',
                                help='Force continuation on errors')
            parser.add_argument('-i', dest='inflight', required=False,
                                default=False, action='store_true',
                                help='Show refs for inflight XDB pages only')
            parser.add_argument('-m', dest='mem', required=False,
                                default=False, action='store_true',
                                help='Dump XDB page memory alloc/free histo instead of ref histo')
            parser.add_argument('-l', dest='last', required=False,
                                default=False, action='store_true',
                                help='Show last n operations instead of histo')


            argv = gdb.string_to_argv(arg)
            self.args = parser.parse_args(argv)

            self.pageTrace()
        except Exception as e:
            print(str(e))
            traceback.print_exc()

class PrintSerListStats(gdb.Command):
    """ PrintSerListStats

    (gdb) PrintSerListStats <pagingListNum>

    Example:
    (gdb) PrintSerListStats 0
    Processing element 0
    Processing element 100000
    [('Resident', 0), ('Serializable', 0), ('Serializing', 0), ('Serialized', 12950), ('Deserializing', 0), ('Dropped', 185794)]
    """
    def __init__(self):
        super(PrintSerListStats, self).__init__("PrintSerListStats", gdb.COMMAND_DATA)

    def invoke(self, arg, from_tty):
        try:
            argv = gdb.string_to_argv(arg)
            if (len(argv) < 1):
                print("PrintSerListStats <pagingListNum>\n")
                return

            serList = gdb.parse_and_eval('XdbMgr::instance->serializationListHead_')
            serListHead = serList[int(argv[0])]
            serListElm = serListHead
            elmNum = 0
            # page states from DataModelTypes.h::XdbPage::PageState
            pageStateNames = ['Resident', 'Serializable', 'Serializing', 'Serialized', 'Deserializing', 'Dropped']
            numXdbPageStates = len(pageStateNames)
            pageStates = [0] * numXdbPageStates
            while serListElm:
                serListElm = serListElm['serializationListNext']
                if not(elmNum % 100000):
                    print("Processing element " + str(elmNum))
                pageState = serListHead['hdr']['pageState']['val']
                pageStates[pageState] += 1
                elmNum += 1
            print (zip(pageStateNames,pageStates))

        except Exception as e:
            print(str(e))
            traceback.print_exc()


XdbPrinter()
GetXdb()
DiffXdb()
XdbPageTrace()
PrintSerListStats()
