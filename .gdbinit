python
import os
import glob
XLRDIR = os.getenv('XLRDIR', os.path.expanduser('~/xcalar'))
gdbScriptDir = os.path.join(XLRDIR, "bin", "gdb")

gdb.execute('directory %s' % XLRDIR)

for pyExtension in glob.glob("{}/*.py".format(gdbScriptDir)):
    gdb.execute("source {}".format(pyExtension))

end
macro define offsetof(t, f) ((size_t)(&((t *) 0)->f))
set print static-members off
set max-value-size unlimited
