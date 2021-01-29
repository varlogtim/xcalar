from multiprocessing import Pool
import traceback


# This call back needs to be a non member of the executor object
def runThis(executor, args):
    try:
        return executor.runUsrFunction(args=args)
    except Exception:
        traceback.print_exc()
        raise


class GenericExecutorArgs(object):
    def __init__(self,
                 classType=None,
                 classConstructorArgs=None,
                 pFnCbName=None,
                 pFnCbArgs=None):
        assert (classType is not None)
        # classConstructorArgs can be NULL
        assert (pFnCbName is not None)
        # pFnCbArgs can be None

        # type of class to be instantiated
        self.classType = classType
        # args packed for class constructor
        self.classConstructorArgs = classConstructorArgs
        # name of the function in above class to be invoked
        self.pFnCbName = pFnCbName
        # arguments packed for the above function
        self.pFnCbArgs = pFnCbArgs


class GenericExecutor(object):
    # runThis will call this function
    def runUsrFunction(self, args):
        assert (args.classType is not None)
        if (args.classConstructorArgs is not None):
            newClass = args.classType(args.classConstructorArgs)
        else:
            newClass = args.classType()

        pFnCb = getattr(newClass, args.pFnCbName)
        if (args.pFnCbArgs is not None):
            return pFnCb(args.pFnCbArgs)
        else:
            return pFnCb()

    # Always infinite wait
    def waitForThread(self, threadHandle):
        result = threadHandle.get()
        assert (threadHandle.ready() is True)
        assert (threadHandle.successful() is True)
        return result

    def runAsync(self, args):
        pool = Pool(processes=1)
        threadHandle = pool.apply_async(runThis, ([self, args]))
        return {"threadHandle":threadHandle, "pool":pool}

    def runInParallelAsync(self, args, numThreads):
        pool = Pool(processes=numThreads)
        threadHandles = []
        for i in range(numThreads):
            threadHandles.append(pool.apply_async(runThis, ([self, args])))
        return {"threadHandles":threadHandles, "pool":pool}

    def runInParallelAndWait(self,
                             args,
                             numThreads,
                             timeOutInSeconds=-1):
        ret = self.runInParallelAsync(
            args=args, numThreads=numThreads)

        threadHandles = ret["threadHandles"]
        pool = ret["pool"]
        results = []
        for thread in threadHandles:
            if (timeOutInSeconds == -1):
                # Caller wants us to wait infinitely
                results.append(thread.get())
            else:
                # Increase the timeout if this throws an exception
                # Do not catch it
                results.append(thread.get(timeout=timeOutInSeconds))

            assert (thread.ready() is True)
            assert (thread.successful() is True)

        pool.close()
        pool.join()

        return results
