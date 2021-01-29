from abc import ABCMeta, abstractmethod
import os
from structlog import get_logger
"""This is an interface to write files to data targets.

"""


class IDataTargetWriter:
    __metaclass__ = ABCMeta

    @classmethod
    def version(self):
        return '1.0'

    @abstractmethod
    def write_data(self, data_target: str, path: str, data: any):
        raise NotImplementedError


"""Writes data to local files instead of the data target

"""


class LocalFileWriter(IDataTargetWriter):
    def __init__(self, local_folder: str):
        self._local_folder = local_folder

    def write_data(self, data_target: str, path: str, data: any):
        write_to_path = os.path.join(self._local_folder, path)
        logger = get_logger(__name__)
        logger.info(f'Writing data to path: {write_to_path}')
        if not os.path.exists(os.path.dirname(write_to_path)):
            os.makedirs(os.path.dirname(write_to_path))
        with open(write_to_path, 'w+') as localFile:
            localFile.write(data)


class DataTargetWriter(object):
    def __init__(self, data_target_writer):
        if not isinstance(data_target_writer, IDataTargetWriter):
            raise Exception('Bad interface')
        if not IDataTargetWriter.version() == '1.0':
            raise Exception('Bad revision')

        self._data_target_writer = data_target_writer

    def write_data(self, data_target: str, path: str, data: any):
        self._data_target_writer.write_data(data_target, path, data)
