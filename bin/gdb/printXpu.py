import gdb
import gdb.printing

class Parent():
    def __init__(self, parent):
        self.parent = parent

    def getXpu(self, parent):
        children = parent["children_"]
        childrenHt = children["slots"]
        numSlots = int(childrenHt.type.sizeof / childrenHt[0].type.sizeof)
        ParentChildType = gdb.lookup_type("ParentChild")
        offset = int(str(gdb.parse_and_eval("&((%s *) 0)->hook_" % "ParentChild")), 0)

        pids = {}
        for ii in range(0, numSlots):
            hook = childrenHt[ii]
            while (int(str(hook), 0) != 0):
                childAddr = int(str(hook), 16) - offset
                child = gdb.Value(childAddr).cast(ParentChildType.pointer())
                pid = int(child["pid"])
                if pid in pids:
                    pids[pid].append((childAddr, child))
                else:
                    pids[pid] = [(childAddr, child)]

                hook = hook["next"]

        return pids

    def printChild(self, childAddr, child):
        print ("<Child (%s): pid: %lu state: %s readyStatus: %s currentParent: %s references: %lu>" % (hex(childAddr), child["pid"], child["state"], child["readyStatus"], child["currentParent_"], child["ref_"]["val"]))

    def findXpu(self, pid):
        parent = self.parent
        pid = int(pid)
        xpus = self.getXpu(parent)

        if pid not in xpus.keys():
            print ("pid %lu not found" % pid)
            return

        for (childAddr, child) in xpus[pid]:
            self.printChild(childAddr, child)

    def printXpu(self):
        parent = self.parent
        xpus = self.getXpu(parent)

        print ("NumChildren: %lu" % parent["childrenCount_"])
        print ("# unique pids: %lu" % len(xpus))

        for xpuList in xpus.values():
            for (childAddr, child) in xpuList:
                self.printChild(childAddr, child)

class XpuPrinter(gdb.Command):
    """ XpuPrinter
    Prints the children processes given a parent object

    (gdb) printXpu <parent> [pid]
    """

    def __init__ (self):
        super (XpuPrinter, self).__init__("printXpu", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        argv = gdb.string_to_argv(arg)
        if (len(argv) < 1):
            print ("printXpu <parent> [pid]")
            return

        parent = Parent(gdb.parse_and_eval(argv[0]))
        if (len(argv) == 1):
            parent.printXpu()
        else:
            parent.findXpu(argv[1])

XpuPrinter()
