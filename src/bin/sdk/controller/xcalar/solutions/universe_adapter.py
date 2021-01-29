import abc


class UniverseAdapter(abc.ABC):
    @abc.abstractmethod
    def getUniverse(self, key):
        pass

    @abc.abstractmethod
    def getSchemas(self, names=[]):
        pass
