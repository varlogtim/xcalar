class RunAsApp(object):
    def shouldRun(self, inTest=False):
        if not inTest:
            import xcalar.container.context as ctx
            if ctx.get_node_id(ctx.get_xpu_id()) != 0:
                return False
        return True

    def ensureDepLocs(self, inTest=False):
        # TODO: This is very hacky.  Remove.
        xlrDir = os.environ["XLRDIR"]
        pathToScheduleApps = os.path.join(xlrDir, "scripts", "scheduleRetinas",
                                                  "apps")
        pathToScheduleSupport = os.path.join(xlrDir, "scripts", "scheduleRetinas",
                                                     "supportModules")
        pathToScheduleCronExecute = os.path.join(xlrDir, "scripts", "scheduleRetinas",
                                                         "executeFromCron")
        if not pathToScheduleApps in sys.path:
            sys.path.append(pathToScheduleApps)
        if not pathToScheduleSupport in sys.path:
            sys.path.append(pathToScheduleSupport)
        if not pathToScheduleCronExecute in sys.path:
            sys.path.append(pathToScheduleCronExecute)
