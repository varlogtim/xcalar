from .WorkItem import WorkItemRuntimeSetParam, WorkItemRuntimeGetParam


class RuntimeParams(object):
    def __init__(self, xcalarApi):
        self.xcalarApi = xcalarApi

    def get(self):
        workItem = WorkItemRuntimeGetParam()
        return self.xcalarApi.execute(workItem)

    def set(self, runtimeSetParams):
        workItem = WorkItemRuntimeSetParam(runtimeSetParams)
        return self.xcalarApi.execute(workItem)
