import time
from structlog import get_logger

logger = get_logger(__name__)


class Timer:
    def __init__(self, msg):
        self.msg = msg

    def __enter__(self):
        logger.debug('Started: ' + self.msg)
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start
        logger.debug(f'Completed: {self.msg}, in {self.interval} seconds')


class TimerMsg:
    def __init__(self, msg, log_start=True):
        self.msg = msg
        self.log_start = log_start

    def __enter__(self):
        if self.log_start:
            logger.info('Started: ' + self.msg)
        self.start = time.time()
        return self

    def __exit__(self, *args):
        pass

    def get_interval(self):
        self.end = time.time()
        return self.end - self.start

    def get(self, msg=None):
        # if rows:
        #     rows = f', Records: {rows}'
        return f'Timer: {self.msg}, {msg}, Interval: {round(self.get_interval(), 3)}'
