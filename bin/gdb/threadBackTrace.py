import gdb
import os

class ThreadBacktrace (gdb.Command):
    """ ThreadBackTrace
    Prints backtrace for all threads not
    blacklisted in src/data/debug/btall_exceptions

    (gdb) tbt [framenum]
    """

    def initInternal(self):
        self.inited = True
        if 'XLRDIR' not in os.environ:
            self.inited = False
            print("\nXLRDIR is not defined, run GDB with 'sudo -E gdb'")

    def __init__ (self):
        super (ThreadBacktrace, self).__init__ ("tbt", gdb.COMMAND_USER)
        self.initInternal()

    def isAllowed(self, bt):
        blackListFilePath = os.environ['XLRDIR'] + "/src/data/debug/btall_exceptions"
        for fn in (fn for fn in os.listdir(blackListFilePath)
                        if not fn.startswith(".")):
            fullName = os.path.join(blackListFilePath, fn)
            with open(fullName, 'r') as traceFile:
                data=traceFile.read()
                # If the exception bt is a substring, ignore the stack trace
                if data in bt:
                    return False
        return True

    def stackString(self, frame):
        btStr = ""
        while frame is not None:
            if frame.name() is not None:
                btStr += frame.name() + "\n"
            frame = frame.older()
        return btStr

    # Crawls up 'num' frames
    def getFrame(self, frame, num):
        while frame is not None and num > 0:
            frame = frame.older()
            num = num - 1
        return frame

    def enumFrame(self, frameNum, threadNum, fiberAddr):
        totalCount = 0
        filteredCount = 0
        firstFrame = gdb.newest_frame()
        btStr = self.stackString(firstFrame)

        if self.isAllowed(btStr):
            desiredFrame = self.getFrame(firstFrame, frameNum)

            if fiberAddr is None:
                prefix = "Thread {0:>3}:".format(str(threadNum))
            else:
                prefix = "((Fiber *) %s)" % fiberAddr

            if desiredFrame is None:
                print("{0:<40} {1}".format(prefix, "Not enough frames"))
            else:
                sal = desiredFrame.find_sal()
                filename = ""
                if sal.symtab is not None:
                    filename = sal.symtab.filename

                    # Strip file so it is just the raw filename
                    filename = os.path.basename(filename)
                else:
                    filename = "unknown file"
                print("{0:<40} {1}:line {2}"
                        .format("%s %s at" % (prefix,
                                               btStr.split('\n', frameNum+1)[frameNum]),
                                filename,
                                sal.line))
        else:
            filteredCount += 1
        totalCount += 1

        return (filteredCount, totalCount)

    def invoke (self, arg, from_tty):
        if not self.inited:
            print("\nXLRDIR is not defined, run GDB with 'sudo -E gdb'")
            return 1

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

        print("Printing frame {} on all threads".format(frameNum))

        this_inferior = gdb.selected_inferior()
        if this_inferior is None:
            print("No attached program")
            return 1
        oldThread = gdb.selected_thread()
        if oldThread is None:
            print("No attached thread")
            return 1
        oldFrame = gdb.selected_frame()
        if oldThread is None:
            print("No attached frame")
            return 1

        threads = this_inferior.threads()
        filteredCount = 0
        totalCount = 0
        for thread in threads:
            thread.switch()
            (threadFilteredCount, threadTotalCount) = self.enumFrame(frameNum, thread.num, None)
            filteredCount += threadFilteredCount
            totalCount += threadTotalCount

        print("{} of {} threads filtered out".format(filteredCount, totalCount))

        oldThread.switch()
        oldFrame.select()

ThreadBacktrace ()
