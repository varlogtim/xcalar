#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include <assert.h>
#include <sys/mman.h>
#include "util/System.h"
#include "util/MemTrack.h"
#include "newtupbuf/NewTuplesCursor.h"

size_t totalSize = 20 * GB;
size_t bufSize = 128 * KB;

uint64_t MAGIC = 0xaabbccdd;

struct ThreadArg {
    uint64_t **buf;
    uint64_t startBuf;
    uint64_t endBuf;
};

struct NewThreadArg {
    NewTuplesBuffer **fTbufs;
    uint64_t startFixedTbuf;
    uint64_t endFixedTbuf;
    NewTupleMeta *tupleMeta;
};

void *
threadWrite(void *args)
{
    ThreadArg *arg = (ThreadArg *) args;

    for (uint64_t ii = arg->startBuf; ii < arg->endBuf; ii++) {
        for (uint64_t jj = 0; jj < bufSize / sizeof(uint64_t); jj++) {
            arg->buf[ii][jj] = MAGIC;
        }
    }

    return NULL;
}

void *
threadRead(void *args)
{
    ThreadArg *arg = (ThreadArg *) args;

    volatile uint64_t n;

    for (uint64_t ii = arg->startBuf; ii < arg->endBuf; ii++) {
        for (uint64_t jj = 0; jj < bufSize / sizeof(uint64_t); jj++) {
            n = arg->buf[ii][jj];
        }
    }

    (void) n;

    return NULL;
}

void *
xdbPack(void *args)
{
    Status status;
    uint64_t numInserts = 0;
    NewThreadArg *arg = (NewThreadArg *) args;

    NewTupleValues tupleValues;
    DfFieldValue valueTmp;
    valueTmp.uint64Val = MAGIC;
    tupleValues.set(0, valueTmp, DfUInt64);

    for (uint64_t ii = arg->startFixedTbuf; ii < arg->endFixedTbuf; ii++) {
        do {
            NewTuplesBuffer *fTbuf = arg->fTbufs[ii];
            status = fTbuf->append(arg->tupleMeta, &tupleValues);
            numInserts++;
        } while (status == StatusOk);
    }

    return (void *) numInserts;
}

void *
xdbCursor(void *args)
{
    Status status;
    NewThreadArg *arg = (NewThreadArg *) args;
    uint64_t numReads = 0;
    size_t numFields = arg->tupleMeta->getNumFields();

    for (uint64_t ii = arg->startFixedTbuf; ii < arg->endFixedTbuf; ii++) {
        NewTuplesBuffer *fTbuf = arg->fTbufs[ii];
        NewTuplesCursor fCursor(fTbuf);
        NewTupleValues fTuplePtr;

        do {
            status = fCursor.getNext(arg->tupleMeta, &fTuplePtr);
            numReads++;
            if (status == StatusOk) {
                bool retIsValid;
                DfFieldValue value =
                    fTuplePtr.get(0, numFields, DfInt64, &retIsValid);
                assert(retIsValid);
                verify(value.uint64Val == MAGIC);
            }
        } while (status == StatusOk);
    }

    return (void *) numReads;
}

Status
xdbBenchmark(unsigned numThreads)
{
    Status status = StatusOk;

    NewThreadArg threadArgs[numThreads];
    pthread_t threads[numThreads];
    pthread_attr_t attr;

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    NewTupleMeta tupleMeta;
    uint64_t ii, jj = 0;
    uint64_t alignment = bufSize;
    size_t numBufs = totalSize / bufSize;

    tupleMeta.setNumFields(1);
    tupleMeta.setFieldType(DfInt64, 0);
    tupleMeta.setFieldsPacking(NewTupleMeta::FieldsPacking::Fixed);

    struct timespec timeStart;
    struct timespec timeEnd;
    unsigned long long diffSecs;
    unsigned long long diffMsecs;
    float gbs;
    uint64_t numInserts = 0, numReads = 0;

    NewTuplesBuffer **fTbufs = new (std::nothrow) NewTuplesBuffer *[numBufs];
    BailIfNull(fTbufs);
    memZero(fTbufs, sizeof(NewTuplesBuffer *) * numBufs);

    for (jj = 0; jj < numBufs; jj++) {
        fTbufs[jj] =
            (NewTuplesBuffer *) memAllocAlignedExt(alignment, bufSize, NULL);
        BailIfNullWith(fTbufs[jj], status);
        mlock(fTbufs[jj], bufSize);
        new (fTbufs[jj]) NewTuplesBuffer((void *) fTbufs[jj], bufSize);
    }

    uint64_t curBuf, remainder;
    curBuf = 0;
    remainder = numBufs % numThreads;

    for (jj = 0; jj < numThreads; jj++) {
        threadArgs[jj].fTbufs = fTbufs;
        threadArgs[jj].startFixedTbuf = curBuf;
        threadArgs[jj].tupleMeta = &tupleMeta;

        curBuf += numBufs / numThreads;
        if (remainder > 0) {
            curBuf++;
            remainder--;
        }

        threadArgs[jj].endFixedTbuf = curBuf;
    }

    clock_gettime(CLOCK_MONOTONIC, &timeStart);

    for (jj = 0; jj < numThreads; jj++) {
        pthread_create(&threads[jj], &attr, xdbPack, &threadArgs[jj]);
    }

    for (jj = 0; jj < numThreads; jj++) {
        void *inserts;
        pthread_join(threads[jj], &inserts);

        numInserts += (uint64_t) inserts;
    }

    clock_gettime(CLOCK_MONOTONIC, &timeEnd);

    diffSecs = timeEnd.tv_sec - timeStart.tv_sec;
    diffMsecs =
        ((diffSecs * NSecsPerSec) + timeEnd.tv_nsec - timeStart.tv_nsec) /
        NSecsPerMSec;

    gbs = (totalSize / GB) / ((float) diffMsecs / MSecsPerSec);

    printf(
        "Tuple buffer: Packing %lu bytes of memory and %lu rows in %llu msecs: "
        "%.2f GB/s\n",
        totalSize,
        numInserts,
        diffMsecs,
        gbs);

    clock_gettime(CLOCK_MONOTONIC, &timeStart);

    for (jj = 0; jj < numThreads; jj++) {
        pthread_create(&threads[jj], &attr, xdbCursor, &threadArgs[jj]);
    }

    for (jj = 0; jj < numThreads; jj++) {
        void *reads;

        pthread_join(threads[jj], &reads);

        numReads += (uint64_t) reads;
    }
    assert(numInserts == numReads);

    clock_gettime(CLOCK_MONOTONIC, &timeEnd);

    diffSecs = timeEnd.tv_sec - timeStart.tv_sec;
    diffMsecs =
        ((diffSecs * NSecsPerSec) + timeEnd.tv_nsec - timeStart.tv_nsec) /
        NSecsPerMSec;

    gbs = (totalSize / GB) / ((float) diffMsecs / MSecsPerSec);

    printf(
        "Tuple buffer: Cursoring %lu bytes of memory of %lu rows in %llu "
        "msecs: %.2f "
        "GB/s\n",
        totalSize,
        numReads,
        diffMsecs,
        gbs);

CommonExit:
    if (fTbufs != NULL) {
        for (ii = 0; ii < numBufs; ii++) {
            if (fTbufs[ii] != NULL) {
                memAlignedFree(fTbufs[ii]);
            }
        }
        delete[] fTbufs;
    }

    return status;
}

int
memoryScanBenchmark(unsigned numThreads)
{
    uint64_t jj;
    size_t numBufs = totalSize / bufSize;

    ThreadArg threadArgs[numThreads];
    pthread_t threads[numThreads];
    pthread_attr_t attr;

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    // @SymbolCheckIgnore
    uint64_t **buf = (uint64_t **) malloc(sizeof(*buf) * numBufs);
    struct timespec timeStart;
    struct timespec timeEnd;

    for (jj = 0; jj < numBufs; jj++) {
        // @SymbolCheckIgnore
        buf[jj] = (uint64_t *) malloc(bufSize);
        if (buf[jj] == NULL) {
            printf("Buf[%lu] is NULL", jj);
            // @SymbolCheckIgnore
            exit(1);
        }
        mlock(buf[jj], bufSize);
    }

    uint64_t curBuf, remainder;
    curBuf = 0;
    remainder = numBufs % numThreads;

    for (jj = 0; jj < numThreads; jj++) {
        threadArgs[jj].buf = buf;
        threadArgs[jj].startBuf = curBuf;

        curBuf += numBufs / numThreads;
        if (remainder > 0) {
            curBuf++;
            remainder--;
        }

        threadArgs[jj].endBuf = curBuf;
    }

    clock_gettime(CLOCK_MONOTONIC, &timeStart);

    for (jj = 0; jj < numThreads; jj++) {
        // @SymbolCheckIgnore
        pthread_create(&threads[jj], &attr, threadWrite, &threadArgs[jj]);
    }

    for (jj = 0; jj < numThreads; jj++) {
        // @SymbolCheckIgnore
        pthread_join(threads[jj], NULL);
    }

    clock_gettime(CLOCK_MONOTONIC, &timeEnd);

    unsigned long long diffSecs = timeEnd.tv_sec - timeStart.tv_sec;
    unsigned long long diffMsecs =
        ((diffSecs * NSecsPerSec) + timeEnd.tv_nsec - timeStart.tv_nsec) /
        NSecsPerMSec;

    float gbs = (totalSize / GB) / ((float) diffMsecs / MSecsPerSec);

    printf("Wrote %lu bytes of memory in %llu msecs %.2f GB/s\n",
           totalSize,
           diffMsecs,
           gbs);

    clock_gettime(CLOCK_MONOTONIC, &timeStart);

    for (jj = 0; jj < numThreads; jj++) {
        // @SymbolCheckIgnore
        pthread_create(&threads[jj], &attr, threadRead, &threadArgs[jj]);
    }

    for (jj = 0; jj < numThreads; jj++) {
        // @SymbolCheckIgnore
        pthread_join(threads[jj], NULL);
    }

    clock_gettime(CLOCK_MONOTONIC, &timeEnd);

    diffSecs = timeEnd.tv_sec - timeStart.tv_sec;
    diffMsecs =
        ((diffSecs * NSecsPerSec) + timeEnd.tv_nsec - timeStart.tv_nsec) /
        NSecsPerMSec;

    gbs = (totalSize / GB) / ((float) diffMsecs / MSecsPerSec);

    printf("Read %lu bytes of memory in %llu msecs: %.2f GB/s\n",
           totalSize,
           diffMsecs,
           gbs);

    return 0;
}

int
main(int argc, char *argv[])
{
    // XXX change this to use getopt
    if (argc < 4) {
        printf(
            "Benchmark type: 1(memory scan) or 2(xdb benchmark)\nNumber of "
            "threads to use\nTotal memory size(in GB)\n");
        exit(1);
    }
    unsigned testType = atoi(argv[1]);
    unsigned numThreads = atoi(argv[2]);
    totalSize = atoi(argv[3]) * GB;

    if (testType == 1) {
        memoryScanBenchmark(numThreads);
    } else if (testType == 2) {
        xdbBenchmark(numThreads);
    } else {
        printf("Invalid bechmark!\n");
        exit(1);
    }
    return 0;
}
