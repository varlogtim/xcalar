class ParseException(Exception):
    pass


class BaseParquetParser(object):
    def __init__(self, parserName, logger):
        self.parserName = parserName
        self.logger = logger

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        pass

    def class_name(self):
        return self.__class__.__name__

    def name(self):
        return self.parserName

    def isAvailable(self):
        return False

    def parseParquet(self,
                     inputPath,
                     inputStream,
                     mode,
                     columns=None,
                     schema=None,
                     capitalizeColumns=False,
                     partitionKeys=None):
        raise NotImplementedError("parseParquet not implemented for {}".format(
            self.class_name()))
