import gdb
import gdb.types
import sys

def getBool(args, num):
    try:
        return args[num] == 'True'
    except IndexError:
        return False

class HashTableCommand(gdb.Command):
    """ HashTableCommand

    Dump and interpret hash tables

    Examples:
    (gdb) hashtable list *BufferCacheMgr::instance->bcHandle_[22]->shadowToBc_ True

    (gdb) hashtable Shadow2Bc

    (gdb) hashtable XdbPagesDbg True
    """
    def __init__(self):
        super(HashTableCommand, self).__init__("hashtable", gdb.COMMAND_DATA)

    def usage(self):
        print("hashtable list <hashTable> [verbose]")
        print("\tLists all elements in hashtable")

        print("hashtable XdbPagesDbg [sortByRefct]")
        print("\tDump B$ to XDB Header map")
        print("\tRequires: cmBuild config debug -DBUFCACHESHADOW=ON")

        print("hashtable Shadow2Bc")
        print("\tDump B$ shadow address to backing address map")
        print("\tRequires: cmBuild config debug -DBUFCACHESHADOW=ON")

        print("hashtable Bc2Shadow")
        print("\tDump B$ backing address to shadow address map")
        print("\tRequires: cmBuild config debug -DBUFCACHESHADOW=ON")

        print("hashtable BcRetired")
        print("\tDump B$ retired address map")
        print("\tRequires: cmBuild config debug -DBUFCACHESHADOW=ON -DBUFCACHESHADOWRETIRED=ON")

        print("hashtable listFields <fieldsRequiredSet>")
        print("\tLists all elements in fieldsRequiredSet")

        print("hashtable listXevalUdfs <registeredFns>")
        print("\tLists all UDFs in the Xcalar Eval registeredFns_ HT")

        print("hashtable listEvalUdfModuleSet <EvalUdfModuleSet>")
        print("\tLists all UDFs in the EvalUdfModuleSet HT")

        print("hashtable listLibNs <LibNsHt>")
        print("\tLists all elements in LibNs HT")

    def invoke(self, arg, from_tty):
        try:
            args = arg.split(" ")
            if len(args) < 1:
                self.usage()
                return

            if args[0] == "list":
                self.listGeneric(args[1],  self.dump, False, getBool(args, 2))
            elif args[0] == "XdbPagesDbg":
                tab = '*XdbMgr::instance->bufCacheDbgToXdbHdrMap'
                self.listGeneric(tab, self.xdbPagesDbg, getBool(args, 1), getBool(args, 2))
            elif args[0] == "Shadow2Bc":
                tab = '*BufferCacheMgr::instance->bcHandle_[22]->shadowToBc_'
                self.listGeneric(tab, self.bcDbg, getBool(args, 1), getBool(args, 2))
            elif args[0] == "Bc2Shadow":
                tab = '*BufferCacheMgr::instance->bcHandle_[22]->bcToShadow_'
                self.listGeneric(tab, self.bcDbg, getBool(args, 1), getBool(args, 2))
            elif args[0] == "BcRetired":
                tab = '*BufferCacheMgr::instance->bcHandle_[22]->retiredShadowToBc_'
                self.listGeneric(tab, self.bcDbg, getBool(args, 1), getBool(args, 2))
            elif args[0] == "listFields":
                self.listFields(" ".join(args[1:]))
            elif args[0] == "listXevalUdfs":
                self.listXevalUdfs(" ".join(args[1:]))
            elif args[0] == "listEvalUdfModuleSet":
                self.listEvalUdfModuleSet(" ".join(args[1:]))
            if args[0] == "listLibNs":
                self.listLibNs(" ".join(args[1:]))
            else:
                self.usage()
        except Exception as e:
            print(str(e))
            traceback.print_exc()

    def xdbPagesDbg(self, arg):
        if type(arg) == list:
            for elm in sorted(arg, reverse=True, key=lambda x: x['refCount']):
                print(elm)
        else:
            fieldVals = {field.name:str(arg[field.name]) for field in arg.type.fields() if field.name != 'hook'}
            xdbPage = gdb.parse_and_eval("(XdbPage *) %s" % fieldVals['valPtr'])
            fieldVals['refCount'] = int(xdbPage['hdr']['refCount']['val'])
            fieldVals['pageState'] = int(xdbPage['hdr']['pageState']['val'])
            return (fieldVals)

    def bcDbg(self, arg):
        if type(arg) == list:
            print("List operations not supported")
        else:
            fieldVals = {field.name:str(arg[field.name]) for field in arg.type.fields() if field.name != 'hook'}
            return(fieldVals)

    # Generic hash table
    def dump(self, arg):
        if type(arg) == list:
            print("List operations not supported")
        else:
            return(arg)

    def listGeneric(self, hashTableName, elmFcn, doList, verbose=False):
        print("\n================================\n")
        print(" %s\n" % hashTableName)
        self.ht = gdb.parse_and_eval(hashTableName)
        self.htType = gdb.types.get_basic_type(self.ht.type)
        self.valueType = self.htType.template_argument(1)
        print("\t\t/* Value type. */                  %s\t" % self.valueType)
        self.hookMember = self.htType.template_argument(2)
        print("\t\t/* Value's hook member. */         %s\t" % self.hookMember)
        self.slotCount = self.htType.template_argument(4)
        print("\t\t/* Slot count. */                  %s\t\t" % self.slotCount)

        self.hookOffset = int(self.valueType[str(self.hookMember).split("::")[-1]].bitpos / 8)
        print("\t\t/* Hook Offset. */                  %s\t\t" % self.hookOffset)

        slot = 0
        fieldValList = []
        while slot < self.slotCount:
            current = gdb.parse_and_eval("(%s).slots[%d]" % (hashTableName, slot))
            while current:
                s = "*('%s' *)(((uint8_t *)0x%lx) - 0x%lx)" % (self.valueType, current, self.hookOffset)
                unused = "%lx"%current # Bizarre noop to make this work, see https://bugs.python.org/issue28023
                item = gdb.parse_and_eval(s)
                fieldVals = elmFcn(item)
                if doList:
                    fieldValList.append(fieldVals)
                else:
                    outStr = ''
                    if verbose:
                        outStr = "[ Slot %d. Hook 0x%lx ] ( %s ): " % (slot, current, s)
                    print(outStr + str(fieldVals))
                current = current["next"]

            slot += 1

        if doList:
            elmFcn(fieldValList)

    def iterAndDump(self, hashTableName):
        slot = 0
        while slot < self.slotCount:
            #print("HashTableName %s slot %d" % (hashTableName, slot))
            current = gdb.parse_and_eval("%s.slots[%d]" % (hashTableName, slot))
            while current:
                #print("valueType %s current 0x%lx hookOffset %d" % (self.valueType, current, self.hookOffset))
                s = "*('%s' *) ((uintptr_t)0x%lx - 0x%lx)" % (self.valueType, current, self.hookOffset)
                print("\n [ Slot %d. Hook 0x%lx ] ( %s )" % (slot, current, s))
                print("--------------------------------")
                print(gdb.parse_and_eval(s))

                current = current["next"]
            slot += 1

    def listFields(self, fieldsRequiredSet):
        print("\n================================\n")
        print(" %s\n" % fieldsRequiredSet)
        hashTableName = fieldsRequiredSet + ".hashTable"

        self.ht = gdb.parse_and_eval(hashTableName)
        self.hookOffset = 264
        self.slotCount = 7
        self.valueType = "Optimizer::Field"
        self.iterAndDump(hashTableName)

    def listXevalUdfs(self, registeredFns):
        print("\n================================\n")
        print(" %s\n" % registeredFns)
        hashTableName = registeredFns;

        self.ht = gdb.parse_and_eval(hashTableName)
        self.hookOffset = 6040
        self.slotCount = 89
        self.valueType = "XcalarEvalRegisteredFn"
        self.iterAndDump(hashTableName)

    def listEvalUdfModuleSet(self, EvalUdfModuleSet):
        print("\n================================\n")
        print(" %s\n" % EvalUdfModuleSet)
        hashTableName = EvalUdfModuleSet;

        self.ht = gdb.parse_and_eval(hashTableName)
        self.hookOffset = 1024
        self.slotCount = 5
        self.valueType = "EvalUdfModule"
        self.iterAndDump(hashTableName)

    def listLibNs(self, LibNsHt):
        print("\n================================\n")
        hashTableName = LibNsHt
        self.ht = gdb.parse_and_eval(hashTableName)
        self.hookOffset = 1032
        self.slotCount = gdb.parse_and_eval("LibNs::PathNameToNsIdHashTableSlot")
        self.valueType = "LibNs::PathNameToNsIdEntry"
        self.iterAndDump(hashTableName)

HashTableCommand()
