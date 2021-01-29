from test_base import TestDriver, TestWorker


# XXX TODO Needs to be implemented
class TestControlPathScaling(TestDriver):
    def __init__(self, test_mgr):
        self.setup_class(test_mgr)

    @classmethod
    def setup_class(cls, test_mgr):
        super().setup_class(test_mgr)

    @classmethod
    def teardown_class(cls, success):
        super().teardown_class(success)

    def num_workers(self):
        return 1

    def work(self):
        self.test_mgr.log_test_header("{}".format(self.__class__.__name__))
        self.test_mgr.start_worker_helper(
            tclass=TestWorkerControlPathScaling,
            num_workers=self.num_workers(),
            args={},
            worker_timeout_sec=self.test_mgr.cliargs.test_duration_in_sec)
        self.test_mgr.log_test_footer("{}".format(self.__class__.__name__))


# XXX TODO Needs to be implemented
# Test Control path scaling worker
class TestWorkerControlPathScaling(TestWorker):
    def __init__(self, test_mgr, tnum, args):
        self.setup_class(test_mgr, tnum, args)

    @classmethod
    def setup_class(cls, test_mgr, tnum, args):
        super().setup_class(test_mgr, tnum, args)
        cls.args = args
        cls.barrier = args["barrier"]

    @classmethod
    def teardown_class(cls, success):
        super().teardown_class(success)

    def work(self):
        self.test_mgr.log_test_header("{}: {}".format(self.__class__.__name__,
                                                      self.tnum))
        if self.barrier.wait() == 0:
            self.logger.info("{} past barrier".format(self.__class__.__name__))
        self.test_mgr.log_test_footer("{}: {}".format(self.__class__.__name__,
                                                      self.tnum))
        return (True, {})
