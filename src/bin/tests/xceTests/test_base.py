# Boiler plate interface base class

import random
from abc import abstractmethod


class TestDriver():
    def __init__(self, test_mgr):
        self.setup_class(test_mgr)

    @classmethod
    def setup_class(cls, test_mgr):
        cls.test_mgr = test_mgr
        cls.test_plan = test_mgr.test_plan
        cls.logger = test_mgr.logger

    @classmethod
    def teardown_class(cls, success):
        pass

    @abstractmethod
    def work(self):
        pass

    @abstractmethod
    def num_workers(self):
        pass


#
# Unit of work executed by Pool of workers.
#
class TestWorker():
    def __init__(self, test_mgr, tnum, args):
        self.setup_class(test_mgr, tnum, args)

    @classmethod
    def setup_class(cls, test_mgr, tnum, args):
        cls.tnum = tnum
        cls.test_mgr = test_mgr
        cls.logger = test_mgr.logger
        cls.args = args
        random.seed(test_mgr.cliargs.test_df_seed + tnum)

    @classmethod
    def teardown_class(cls, success):
        pass

    @abstractmethod
    def work(self):
        pass
