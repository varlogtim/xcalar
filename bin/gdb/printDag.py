import gdb
import gdb.printing

DagNodeTypeStr = "DagNodeTypes::Node"

def getCStr(data):
    outputStr = data.string("ascii")
    return outputStr

def fnv64(dataStr):
    data = dataStr.encode("ascii")
    hash_ = 0xcbf29ce484222325
    for b in data:
        if (type(b) is int):
            b
        else:
            b = ord(b)
        hash_ ^= b
        hash_ *= 0x100000001b3
        hash_ &= 0xffffffffffffffff
    return hash_

class DagNodePrinter(gdb.Command):
    """ DagNodePrinter

    (gdb) printDagNode dagNode
    """
    def __init__(self):
        super(DagNodePrinter, self).__init__("printDagNode", gdb.COMMAND_DATA)

    def to_string(self, arg):
        dagNodeHdr = arg["dagNodeHdr"]
        apiDagNodeHdr = dagNodeHdr["apiDagNodeHdr"]
        dagNodeName = getCStr(apiDagNodeHdr["name"])
        dagNodeId = int(str(apiDagNodeHdr["dagNodeId"]))
        state = str(apiDagNodeHdr["state"])
        dagNodeApi = apiDagNodeHdr["api"]
        nextNodeId = int(str(dagNodeHdr["dagNodeOrder"]["next"]))
        prevNodeId = int(str(dagNodeHdr["dagNodeOrder"]["prev"]))
        numParents = int(str(arg["numParent"]))
        numChildren = int(str(arg["numChild"]))
        return "<%s (%d) %s addr: ((DagNodeTypes::Node *) %s) next: %d prev: %d numParents: %d numChildren: %d State: %s>" % (dagNodeName, dagNodeId, str(dagNodeApi), dagNodeHdr.address, nextNodeId, prevNodeId, numParents, numChildren, state)

    def invoke(self, arg, from_tty):
        print("%s" % self.to_string(gdb.parse_and_eval(arg)))

class DagPrinter(gdb.Command):
    """ DagPrinter

    (gdb) printDag dagHandle
    """

    def __init__(self):
        super(DagPrinter, self).__init__("printDag", gdb.COMMAND_DATA)
        self.val = None

    def getDagNode(self, dagNodeIdIn):
        dagHdr = self.val["hdr_"]
        idHashTable = self.val["idHashTable_"]
        slotNum = (int(dagNodeIdIn) % int(self.val["IdHashTableSlots"]))
        hook = idHashTable["slots"][slotNum]
        found = False
        offset = int(str(gdb.parse_and_eval("&(('%s' *) 0)->idHook" % DagNodeTypeStr)), 0)
        DagNodeType = gdb.lookup_type(DagNodeTypeStr)

        while (int(str(hook), 0) != 0):
            dagNodeAddr = int(str(hook), 16) - offset
            dagNode = gdb.Value(dagNodeAddr).cast(DagNodeType.pointer())
            dagNodeId = int(dagNode["dagNodeHdr"]["apiDagNodeHdr"]["dagNodeId"])
            if (dagNodeId == dagNodeIdIn):
                found = True
                break
            hook = hook["next"]

        if not found:
            raise Exception("DagNode %d not found" % dagNodeIdIn)

        return dagNode

    def to_string(self, arg):
        self.val = arg
        dagHdr = self.val["hdr_"];
        numNodes = int(str(dagHdr["numNodes"]))
        numSlots = int(self.val["IdHashTableSlots"])
        firstNode = int(str(dagHdr["firstNode"]))
        idHashTable = self.val["idHashTable_"]
        outputStr = "NumNodes: %d NumSlots: %d FirstNode: %d\n" % (numNodes, numSlots, firstNode)
        dagNodeId = firstNode
        for ii in range(0, numNodes):
            dagNode = self.getDagNode(dagNodeId)
            outputStr += "Node %d (idSlot: %d, nameSlot: %d): %s\n" % (ii,
                          dagNodeId % numSlots,
                          fnv64(getCStr(dagNode["dagNodeHdr"]["apiDagNodeHdr"]["name"])) % numSlots,
                                        DagNodePrinter.to_string(DagNodePrinter(), dagNode))
            dagNodeId = int(str(dagNode["dagNodeHdr"]["dagNodeOrder"]["next"]))

        return outputStr

    def invoke(self, arg, from_tty):
        argv = gdb.string_to_argv(arg)
        if (len(argv) < 1):
            print("printDag dagHandle [dagNodeId]")
            return

        try:
            self.val = gdb.parse_and_eval(argv[0])
            if (len(argv) == 1):
                print("%s" % self.to_string(self.val))
            else:
                dagNodeId = int(argv[1])
                dagNode = self.getDagNode(dagNodeId)
                numParents = int(dagNode["numParent"])
                numChildren = int(dagNode["numChild"])
                print("%s" % DagNodePrinter.to_string(DagNodePrinter(), dagNode))
                parentListElt = dagNode["parentsList"]
                childrenListElt = dagNode["childrenList"]
                print ("\nParents:")
                for ii in range(0, numParents):
                    parentDagNodeId = gdb.Value(parentListElt).cast(gdb.lookup_type("Xid").pointer()).dereference()
                    parentDagNode = self.getDagNode(parentDagNodeId)
                    print("  Parent %d: %s" % (ii, DagNodePrinter.to_string(DagNodePrinter(), parentDagNode)))

                    parentListElt = gdb.parse_and_eval("*(void **)((uintptr_t)%s + 8)" % str(parentListElt))
                print ("\nChildren:")
                for ii in range(0, numChildren):
                    childDagNodeId = gdb.Value(childrenListElt).cast(gdb.lookup_type("Xid").pointer()).dereference()
                    childDagNode = self.getDagNode(childDagNodeId)
                    print("  Child %d: %s" % (ii,
                          DagNodePrinter.to_string(DagNodePrinter(), childDagNode)))
                    childrenListElt = gdb.parse_and_eval("*(void **)((uintptr_t)%s + 8)" % str(childrenListElt))
        except:
            self.val = gdb.parse_and_eval(arg)
            print("%s" % self.to_string(self.val))


DagPrinter()
DagNodePrinter()
