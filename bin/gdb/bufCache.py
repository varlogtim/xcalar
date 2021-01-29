import gdb
import gdb.types
import re
import traceback

from collections import Counter

class BufCacheDump(gdb.Command):
    """ BufCacheDump

    (gdb) bc <command> <args>

    Commands:
        list:       List all slabs and per-slab stats
        xcontexts:  (preferred) Dump allocation contexts for given slab
        contexts:   Dump allocation contexts for given slab

    Requirements:
        xcontexts:  Enable XDB page tracing config parameter (no recompile needed)
        contexts:   cmBuild config debug -DBUFCACHETRACE=ON

    Examples:

    (gdb) bc list
    (gdb) bc xcontexts 22
    (gdb) bc contexts 22
    """
    def __init__(self):
        super(BufCacheDump, self).__init__("bc", gdb.COMMAND_DATA)

    def usage(self):
        print('Try "help bc"')

    def invoke(self, arg, from_tty):
        try:
            argv = gdb.string_to_argv(arg)
            if len(argv) == 0 or argv[0] == "list":
                self.listSlabs()
            elif argv[0] == "contexts" and len(argv) == 2:
                slabNum = int(argv[1])
                self.allocContexts(slabNum)
            elif argv[0] == "xcontexts" and len(argv) == 2:
                slabNum = int(argv[1])
                self.xallocContexts(slabNum)
            else:
                self.usage()
        except Exception as e:
            print(str(e))
            traceback.print_exc()

    def listSlabs(self):
        bcMgr = gdb.parse_and_eval("*BufferCacheMgr::instance")
        objCt = bcMgr['objectCount_']
        fmtStr = "{:<10} {:<20} {:<10} {:<10} {:<10} {}"
        print(fmtStr.format("Slab", "BcHandle", "Buf Size", "# Bufs", "# Alloc'd", "Name"))
        for i in range(objCt):
            handle = bcMgr['bcHandle_'][i]
            obj = handle['object_']
            allocs = handle['fastAllocs_']['statUint64']
            frees = handle['fastFrees_']['statUint64']
            objName = str(obj['objectName_']).split(' ')[1]
            print(fmtStr.format(i, str(handle), str(obj['bufferSize_']), str(obj['numElements_']), str(allocs - frees), objName))

    def allocContexts(self, slabNum):
        bcMgr = gdb.parse_and_eval("*BufferCacheMgr::instance")
        handle = bcMgr['bcHandle_'][slabNum]
        bcHdrs = handle['freeListSaved_']
        obj = handle['object_']
        numElms = obj['numElements_']
        numTraces = handle['NumTraces']
        objName = str(obj['objectName_']).split(' ')[1]

        traceList = []
        for elm in range(numElms):
            idx = bcHdrs[elm]['idx_']
            isAlloc = bcHdrs[elm]['bufState_']

            # Only show allocated buffers
            if isAlloc != 1:
                continue
            if idx == 0:
                idx = numTraces - 1
            else:
                idx -= 1
            traceList.append(str(bcHdrs[elm]['traces_'][idx]['trace']))
        traceFreq = Counter(traceList)
        print("{} elements, {} unique contexts".format(numElms, len(traceFreq)))
        for k in sorted(traceFreq, key=traceFreq.get, reverse=True):
            trace = re.sub(r'\(.*?\)', r'', k)
            trace = '>\n'.join(trace.split('>,'))
            print("======== {:^7} ========".format(traceFreq[k]))
            print ("{}".format(trace))

    def xallocContexts(self, slabNum):
        hashTableName = '*globalTraces_->tracesMemInFlight'
        traces = gdb.parse_and_eval("globalTraces_->tracesAllocs->refCtx")
        self.ht = gdb.parse_and_eval(hashTableName)
        self.htType = gdb.types.get_basic_type(self.ht.type)
        self.valueType = self.htType.template_argument(1)
        self.hookMember = self.htType.template_argument(2)
        self.slotCount = self.htType.template_argument(4)

        self.hookOffset = int(self.valueType[str(self.hookMember).split("::")[-1]].bitpos / 8)

        slot = 0
        ctxNumList = []
        while slot < self.slotCount:
            current = gdb.parse_and_eval("(%s).slots[%d]" % (hashTableName, slot))
            while current:
                s = "*('%s' *)(((uint8_t *)0x%lx) - 0x%lx)" % (self.valueType, current, self.hookOffset)
                unused = "%lx"%current # Bizarre noop to make this work, see https://bugs.python.org/issue28023
                item = gdb.parse_and_eval(s)
                # In this method each val corresponds to an integer context index
                ctxNumList.append(int(item['val']))
                current = current["next"]

            slot += 1

        traceFreq = Counter(ctxNumList)
        print("{} elements, {} unique contexts".format(sum(traceFreq.values()), len(traceFreq)))
        for k in sorted(traceFreq, key=traceFreq.get, reverse=True):
            traceStr = str(traces[k]['trace'])
            trace = re.sub(r'\(.*?\)', r'', traceStr)
            trace = '>\n'.join(trace.split('>,'))
            print("======== {:^7} ========".format(traceFreq[k]))
            print ("{}".format(trace))




BufCacheDump()
