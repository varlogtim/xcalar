import gdb
import gdb.types

class BreakAtFunction(gdb.Command):
    """ BreakAtFunction

    (gdb) breakAtFunction <funcName>
    """
    def __init__(self):
        super(BreakAtFunction, self).__init__("breakAtFunction", gdb.COMMAND_DATA)

    def usage(self):
        print("breakAtFunction <funcName>")
        print("\tBreak at all the matches for a given funcName")

    def invoke(self, arg, from_tty):
        args = arg.split(" ")
        if len(args) < 1:
            self.usage()
            return
        self.list(" ".join(args[0:]))

    def list(self, funcName):
        print("\nGiven function pattern: %s\n" % funcName)
        print("================================")
        funcSymbols = gdb.execute('info functions ' + funcName, to_string=True).split('\n')
        count = 0
        for funcSym in funcSymbols:
            if "All functions matching regular expression" in funcSym or not funcSym:
                continue
            if funcName in funcSym:
                count = count + 1
                print(" %d: %s\n" % (count, funcSym))
                gdb.execute('break ' + funcSym[:-1])
                print("---")
        print("================================")

BreakAtFunction()
