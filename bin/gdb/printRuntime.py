import gdb
import gdb.printing

def makeGdbExecuteCmd(command, from_tty, to_string):
    return { "fn": "gdb.execute",
            "args": {
                "command": command,
                "from_tty": from_tty,
                "to_string": to_string
                }
            }

def makeEnumFrameCmd(tbtObj, frameNum, fiberAddr):
    return { "fn": "ThreadBacktrace.enumFrame",
            "args": {
                "self": tbtObj,
                "frameNum": frameNum,
                "fiberAddr": fiberAddr,
                "threadNum": None
                }
           }

def evaluate(cmd):
    selfDefined = False
    if len(cmd["fn"].split(".")) > 1 and cmd["fn"].split(".")[0] != "gdb":
        fnName=cmd["fn"].split(".")[1]
        instance=cmd["fn"].split(".")[0]
        evalString = cmd["fn"] + "(cmd[\"args\"][\"self\"],"
        selfDefined = True
    else:
        evalString = cmd["fn"] + "("
    args = []
    for key in cmd["args"]:
        if selfDefined is True and key == "self":
            continue
        args.append("%s = cmd[\"args\"][\"%s\"]" % (key, key))
    evalString += ",".join(args) + ")"
    try:
        return (eval(evalString))
    except KeyboardInterrupt as exc:
        raise exc
    except:
        print("Error executing %s: %s" % (evalString, sys.exc_info()[0]))
        print("%s" % sys.exc_info()[1])

def getCStr(data):
    outputStr = data.string("ascii")
    return outputStr

def getFiberRegs(savedContext):
    ucMContext = savedContext["uc_mcontext"]
    gregs = ucMContext["gregs"].address.cast(gdb.lookup_type("unsigned long long").pointer())
    # Refer to /usr/include/x86_64-linux-gnu/sys/ucontext.h
    return {"r8": gregs[0],
            "r9": gregs[1],
            "r10": gregs[2],
            "r11": gregs[3],
            "r12": gregs[4],
            "r13": gregs[5],
            "r14": gregs[6],
            "r15": gregs[7],
            "rdi": gregs[8],
            "rsi": gregs[9],
            "rbp": gregs[10],
            "rbx": gregs[11],
            "rdx": gregs[12],
            "rax": gregs[13],
            "rcx": gregs[14],
            "rsp": gregs[15],
            "rip": gregs[16],
            "eflags": gregs[17] & 0xffffffff }

def getRuntimeTypeNames():
    rtTypeNames = []
    rtTypeNames.append(str(gdb.parse_and_eval("'Runtime::SchedId::Sched0'")))
    rtTypeNames.append(str(gdb.parse_and_eval("'Runtime::SchedId::Sched1'")))
    rtTypeNames.append(str(gdb.parse_and_eval("'Runtime::SchedId::Sched2'")))
    rtTypeNames.append(str(gdb.parse_and_eval("'Runtime::SchedId::Immediate'")))
    rtTypeNames.append(str(gdb.parse_and_eval("'Runtime::SchedId::SendMsgNw'")))
    rtTypeNames.append(str(gdb.parse_and_eval("'Runtime::SchedId::AsyncPager'")))
    return rtTypeNames

class FiberSwitcher(gdb.Command):
    """ FiberSwitcher

    (gdb) fiber enter <fiber>
    (gdb) fiber <gdbCommand>
    (gdb) ...
    (gdb) fiber exit
    """
    def __init__(self):
        super(FiberSwitcher, self).__init__("fiber", gdb.COMMAND_DATA)
        self.currentFiber = None
        self.savedFiberRegs = None
        self.currentFrame = None

    def enterFiber(self, arg):
        fiber = gdb.parse_and_eval(arg)
        state = fiber["state_"]
        if (str(state) != "Fiber::State::Suspended"):
            print("Cannot enter a fiber that's not suspended")
            return
        savedContext = fiber["savedContext_"]
        self.savedFiberRegs = getFiberRegs(savedContext)
        self.currentFiber = fiber
        self.currentFrame = None
        print ("Entered %s" % fiber)

    def showFiber(self):
        print ("Current fiber: %s Frame: %s" % (self.currentFiber, self.currentFrame))

    def exitFiber(self):
        self.currentFiber = None
        self.savedFiberRegs = None

    def executeInFiber(self, cmd, fiberRegs, fiberFrame):
        keyboardException = None
        savedFrame = gdb.selected_frame()
        gdb.execute("select-frame 0")
        savedRegs = dict();
        for reg in fiberRegs:
            if reg != "eflags":
                curRegVal = gdb.parse_and_eval("(uint64_t) $%s" % reg)
            else:
                curRegVal = gdb.parse_and_eval("(uint32_t) $%s" % reg)
            savedRegs[reg] = curRegVal

        try:
            # Swap over to fiber
            for reg in fiberRegs:
                try:
                    gdb.execute("set $%s = %s" % (reg, fiberRegs[reg]))
                except:
                    print ("Error setting %s = %s" % (reg, fiberRegs[reg]))

            if (fiberFrame is not None):
                fiberFrame.select()

            evaluate(cmd)

            fiberFrame = gdb.selected_frame();
        except KeyboardInterrupt as exc:
            keyboardException = exc
        except:
            print ("Fiber error: %s" % sys.exc_info()[0])

        # Restore regs
        gdb.execute("select-frame 0")
        for reg in savedRegs:
            try:
                gdb.execute("set $%s = %s" % (reg, savedRegs[reg]))
            except:
                print ("Error setting %s = %s" % (reg, savedRegs[reg]))

        # Restore frame
        savedFrame.select()

        if keyboardException is not None:
            raise keyboardException

        return fiberFrame

    def execute(self, arg):
        cmd = makeGdbExecuteCmd(arg, True, False)
        self.currentFrame = self.executeInFiber(cmd, self.savedFiberRegs, self.currentFrame)

    def invoke(self, arg, from_tty):
        args = arg.split(" ")
        if (len(args) < 1):
            print("fiber enter <fiber>\nfiber <gdbCommand>\nfiber exit")
            return

        if (args[0] == "enter"):
            self.enterFiber(" ".join(args[1:]))
        elif (args[0] == "exit"):
            self.exitFiber()
        elif (args[0] == "show"):
            self.showFiber()
        else:
            if (self.savedFiberRegs is None):
                print("Not currently in any fiber. Do fiber enter <fiber> first")
                return
            self.execute(arg)

class FiberPrinter(gdb.Command):
    """ FiberPrinter

    (gdb) printFiber <fiber>
    """
    def __init__(self):
        super(FiberPrinter, self).__init__("printFiber", gdb.COMMAND_DATA)

    def to_string(self, arg):
        state = arg["state_"];
        currentSchedObj = arg["currentSchedObj_"]
        stackBase = arg["stackBase_"]
        stackUserBase = arg["stackUserBase_"]
        returnStr=("State: %s, currentSchedObj: %s, stackBase: %s, stackUserBase: %s" %
                   (state, currentSchedObj, stackBase, stackUserBase))

        if (str(state) == "Fiber::State::Suspended"):
            savedContext = arg["savedContext_"]
            fiberRegs = getFiberRegs(savedContext)
            cmd = makeGdbExecuteCmd("bt", False, False)
            FiberSwitcher().executeInFiber(cmd, fiberRegs, None)

        return (returnStr)

    def invoke(self, arg, from_tty):
        print ("%s" % self.to_string(gdb.parse_and_eval(arg)))

class SchedObjPrinter(gdb.Command):
    """ SchedObjPrinter

    (gdb) printSchedObj <schedObj>
    """
    def __init__(self):
        super(SchedObjPrinter, self).__init__("printSchedObj", gdb.COMMAND_DATA)

    def to_string(self, arg):
        state = arg["state_"]
        fiber = "(Fiber *)%s" % arg["fiber_"]
        schedulable = "(Schedulable *)%s" % arg["schedulable_"]

        # If suspended, get the details.
        try:
            sem = arg["sem"]
            fileName = getCStr(arg["fileName"])
            funcName = getCStr(arg["funcName"])
            lineNumber = int(arg["lineNumber"])
        except:
            sem = None

        try:
            suspendCount = int(arg["notRunnableCount"])
        except:
            suspendCount = None

        s = "<(SchedObject *)%s state: %s, fiber: %s, schedulable: %s" % (arg, state, fiber, schedulable)
        if suspendCount:
            s += ", suspendCount: %d" % suspendCount
        if sem:
            s += ", suspended @%s:%d (%s) on (Semaphore *)%s" % (fileName, lineNumber, funcName, sem)

        s += ">"
        return s

    def invoke(self, arg, from_tty):
        print ("%s" % self.to_string(gdb.parse_and_eval(arg)))

class RuntimePrinter(gdb.Command):
    """ RuntimePrinter

    (gdb) runtime sched0ListNr
    (gdb) runtime sched0ListR
    (gdb) runtime sched1ListNr
    (gdb) runtime sched1ListR
    (gdb) runtime sched2ListNr
    (gdb) runtime sched2ListR
    (gdb) runtime immediateListNr
    (gdb) runtime immediateListR
    (gdb) runtime sendMsgNwListR
    (gdb) runtime sendMsgNwListNr
    (gdb) runtime asyncPagerListR
    (gdb) runtime asyncPagerListNr
    """
    def __init__(self):
        super(RuntimePrinter, self).__init__("runtime", gdb.COMMAND_DATA)

    def listNotRunnable(self, rtTypeEnum, printResult=True):
        # Get reference to global scheduler.
        sched = gdb.parse_and_eval("(('Runtime::SchedInstance' *)(&Runtime::instance->schedInstance_) + '%s')->scheduler" % rtTypeEnum)

        fibers = []

        count = 0
        current = sched["notRunnableQHead"]
        while current:
            fibers.append(current["fiber_"])
            print("\n----------------------------------------------\n")
            print("Fiber: %d\n" % count)
            FiberPrinter().to_string(current["fiber_"])
            count += 1
            current = current["nextNotRunnable_"]
        print("\n----------------------------------------------\n")
        print("Runtime %s, found %d not runnable SchedObjects" % (rtTypeEnum, count))

        if printResult is True:
            count = 0
            current = sched["notRunnableQHead"]
            while current:
                print("SchedObject %d (addr: %s): %s" %
                      (count, current,
                       SchedObjPrinter().to_string(current)))
                count += 1
                current = current["nextNotRunnable_"]

        return fibers

    def listRunnable(self, rtTypeEnum):
        # Get reference to global scheduler.
        sched = gdb.parse_and_eval("(('Runtime::SchedInstance' *)(&Runtime::instance->schedInstance_) + '%s')->scheduler" % rtTypeEnum)

        count = 0
        current = sched["runnableQHead"]
        while current:
            count += 1
            current = current["nextRunnable_"]
        print("Runtime %s, found %d runnableQHead (but not running) SchedObjects" % (rtTypeEnum, count))

        count = 0
        current = sched["runnableQHead"]
        while current:
            print("SchedObject %d (addr: %s): %s" %
                  (count, current,
                   SchedObjPrinter().to_string(current)))
            count += 1
            current = current["nextRunnable_"]

        count = 0
        current = sched["runnableFiberQHead"]
        while current:
            count += 1
            current = current["nextRunnable_"]
        print("Runtime %s, found %d runnableFiberQHead (but not running) SchedObjects" % (rtTypeEnum, count))

        count = 0
        current = sched["runnableFiberQHead"]
        while current:
            print("SchedObject %d (addr: %s): %s" %
                  (count, current,
                   SchedObjPrinter().to_string(current)))
            count += 1
            current = current["nextRunnable_"]

    def invoke(self, arg, from_tty):
        scheduler = ""
        runnable = False
        if arg == "sched0ListNr":
            scheduler = "Sched0"
            runnable = False
        elif arg == "sched0ListR":
            scheduler = "Sched0"
            runnable = True
        elif arg == "sched1ListNr":
            scheduler = "Sched1"
            runnable = False
        elif arg == "sched1ListR":
            scheduler = "Sched1"
            runnable = True
        elif arg == "sched2ListNr":
            scheduler = "Sched2"
            runnable = False
        elif arg == "sched2ListR":
            scheduler = "Sched2"
            runnable = True
        elif arg == "immediateListNr":
            scheduler = "Immediate"
            runnable = False
        elif arg == "immediateListR":
            scheduler = "Immediate"
            runnable = True
        elif arg == "sendMsgNwListNr":
            scheduler = "SendMsgNw"
            runnable = False
        elif arg == "sendMsgNwListR":
            scheduler = "SendMsgNw"
            runnable = True
        elif arg == "asyncPagerListNr":
            scheduler = "AsyncPager"
            runnable = False
        elif arg == "asyncPagerListR":
            scheduler = "AsyncPager"
            runnable = True
        else:
            print("Welcome to the 'runtime' help menu!")
            print("    Type 'runtime sched0ListNr' to see not runnable 'Sched0' SchedObjects")
            print("    Type 'runtime sched0ListR' to see runnable (but not running) 'Sched0' SchedObjects")
            print("    Type 'runtime sched1ListNr' to see not runnable 'Sched1' SchedObjects")
            print("    Type 'runtime sched1ListR' to see runnable (but not running) 'Sched1' SchedObjects")
            print("    Type 'runtime sched2ListNr' to see not runnable 'Sched2' SchedObjects")
            print("    Type 'runtime sched2ListR' to see runnable (but not running) 'Sched2' SchedObjects")
            print("    Type 'runtime immediateListNr' to see not runnable 'Immediate' SchedObjects")
            print("    Type 'runtime immediateListR' to see runnable (but not running) 'Immediate' SchedObjects")
            print("    Type 'runtime sendMsgNwListNr' to see not runnable 'SendMsgNw' SchedObjects")
            print("    Type 'runtime sendMsgNwListR' to see runnable (but not running) 'SendMsgNw' SchedObjects")
            print("    Type 'runtime asyncPagerListNr' to see not runnable 'AsyncPager' SchedObjects")
            print("    Type 'runtime asyncPagerListR' to see runnable (but not running) 'AsyncPager' SchedObjects")
            return

        rtTypeNames = getRuntimeTypeNames()
        for rtTypeName in rtTypeNames:
            if scheduler.lower() in rtTypeName.lower():
                if runnable:
                    self.listRunnable(rtTypeName)
                else:
                    self.listNotRunnable(rtTypeName)

class FiberBacktrace (gdb.Command):
    """ FiberBackTrace
    Prints backtrace for all fibers
    not blacklisted in src/data/debug/btall_exceptions

    (gdb) fbt [framenum]
    """

    def __init__ (self):
        super (FiberBacktrace, self).__init__("fbt", gdb.COMMAND_USER)
        self.tbt = None

    def invoke(self, arg, from_tty):
        frameNum = 0
        if (arg != ""):
            try:
                frameNum = int(arg)
            except ValueError:
                print("'{}' is not a valid frame number".format(arg))
                return 1

        if frameNum < 0:
            print("frame number must be non-negative")
            return 1

        if not self.tbt:
            tbt = ThreadBacktrace()
            tbt.initInternal()
            self.tbt = tbt

        rtTypeNames = getRuntimeTypeNames()
        for rtTypeName in rtTypeNames:
            if "Max".lower() in rtTypeName.lower():
                continue
            fibers = RuntimePrinter().listNotRunnable(rtTypeName, False)

            for fiber in fibers:
                savedContext = fiber["savedContext_"]
                fiberRegs = getFiberRegs(savedContext)
                cmd = makeEnumFrameCmd(self.tbt, frameNum, str(fiber))
                try:
                    FiberSwitcher().executeInFiber(cmd, fiberRegs, None)
                except KeyboardInterrupt:
                    break

RuntimePrinter()
SchedObjPrinter()
FiberPrinter()
FiberSwitcher()
FiberBacktrace()
