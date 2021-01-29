import sys
import os
import psutil
import logging

from socket import gethostname

PROCESS = psutil.Process(os.getpid())
KB = 10**3
MB = KB * KB
LARGE_INT = 8 * MB
LARGE_STR = ' ' * 8 * MB

logger = logging.getLogger("xpu")
logger.setLevel(logging.INFO)
if not logger.handlers:
    # Logging is not multi-process safe, so stick to stderr
    log_handler = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter(
        '%(asctime)s - Test OOM App - %(levelname)s - %(message)s')
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)
    logger.debug("Test OOM App initialized; hostname:{}", gethostname())


def pmem():
    try:
        tot, avail, percent, used, free, active, inactive, buffers = \
                psutil.virtual_memory()
    except ValueError:
        tot, avail, percent, used, free, active, inactive, buffers, cached, shared = \
                psutil.virtual_memory()
    tot, avail, used, free = tot / MB, avail / MB, used / MB, free / MB
    proc = PROCESS.memory_info()[1] / MB
    logger.info(
        "MemStats: process = {} total = {} avail = {} used = {} free = {} percent = {}"
        .format(proc, tot, avail, used, free, percent))


def alloc_max_array():
    logger.info("Start alloc_max_array")
    i = 0
    ar = []
    while True:
        try:
            ar.append(LARGE_STR + str(i))
        except MemoryError:
            break
        i += 1
    max_i = i - 1
    logger.info(
        "End alloc_max_array, maximum array allocation = {}".format(max_i))


def alloc_max_str():
    logger.info("Start alloc_max_str")
    i = 0
    while True:
        try:
            a = ' ' * (i * LARGE_INT)
            del a
        except MemoryError:
            break
        i += 1
    max_i = i - 1
    _ = ' ' * (max_i * LARGE_INT)
    logger.info(
        "End alloc_max_int, maximum string allocation = {}".format(max_i))


def main(inBlob):
    pmem()
    alloc_max_str()
    pmem()
    alloc_max_array()
    pmem()
