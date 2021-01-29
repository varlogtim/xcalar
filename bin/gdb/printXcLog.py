import gdb
import gdb.printing

class xcalarLogPrinter(gdb.Command):
    """ xcalarLogPrinter
    Prints Xcalar Logs from in-core buffer. Works only on prod/non-DEBUG builds!

    (gdb) printXcalarLog
    """

    def __init__ (self):
        super (xcalarLogPrinter, self).                                     \
                                __init__("printXcalarLog", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        loglist = gdb.parse_and_eval("xcalarLogBuffer").string().split('\n')
        for lgix in range(1, len(loglist)):
            print ('%s' % loglist[lgix-1])

xcalarLogPrinter()
