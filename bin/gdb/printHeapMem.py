import gdb
import gdb.types
from contextlib import contextmanager
import ctypes
import io
import os, sys
import tempfile

class HeapMemCommand(gdb.Command):
    """ HeapMemCommand
    (gdb) heap list
    """
    def __init__(self):
        super(HeapMemCommand, self).__init__("heap", gdb.COMMAND_DATA)

    def usage(self):
        print("heap list")
        print("\tLists all heap allocations")

    def invoke(self, arg, from_tty):
        args = arg.split(" ")
        if len(args) < 1:
            self.usage()
            return
        if args[0] == "list":
            self.list()
        else:
            self.usage()

    def list(self):
        evalStr = "call (void)malloc_stats()"
        print("{}".format(evalStr))
        gdb.execute(evalStr)

HeapMemCommand()
