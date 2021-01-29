#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include <assert.h>
#include <sys/mman.h>
#include <getopt.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "strings/String.h"
#include "util/System.h"

/* Flag set by ‘--verbose’. */
static int verboseFlag;
/* Flag set by ‘--help’. */
static int helpFlag;
static uint64_t ioSizeInBytes;
static uint32_t numThreads;
static uint32_t numIters;
static const uint32_t IOBUFSIZE = 4 * 1024;
static char ioPath[IOBUFSIZE];

enum ThreadWork {
    Reader,
    Writer,
};

struct ThreadArg {
    ThreadWork work;
    unsigned threadNum;
    uint64_t ioBytesDone;
    uint32_t iters;
};

static uint64_t ioBytesDoneRead;
static uint32_t itersRead;
static uint64_t ioBytesDoneWrite;
static uint32_t itersWrite;

static void printHelp();

void *
ioThread(void *args)
{
    Status status;
    ThreadArg *tArgs = (ThreadArg *) args;
    ThreadWork work = tArgs->work;
    unsigned threadNum = tArgs->threadNum;
    uint64_t curIters = 0;
    uint64_t totalBytes = 0;
    void *buffer = NULL;
    int fd = -1;

    char ioPathCur[IOBUFSIZE];

    snprintf(ioPathCur, IOBUFSIZE, "%s/%u", ioPath, threadNum);

    if (work == Writer) {
        fd = open(ioPathCur, O_CREAT | O_RDWR, S_IRWXU | S_IRWXG | S_IRWXO);
    } else {
        fd = open(ioPathCur, O_RDONLY, S_IRWXU | S_IRWXG | S_IRWXO);
    }
    if (fd == -1) {
        status = sysErrnoToStatus(errno);
        printf("Failed open %s: %s", ioPathCur, strGetFromStatus(status));
        exit(-1);
    }

    if (verboseFlag) {
        printf("Issue %s: numIters %u, ioSizeInBytes %lu\n",
               work == Reader ? "Reads" : "Writes",
               numIters,
               ioSizeInBytes);
    }

    buffer = malloc(ioSizeInBytes);
    if (!buffer) {
        printf("Failed to allocate buffer size %lu", ioSizeInBytes);
        exit(-1);
    }

    memset(buffer, 0xae, ioSizeInBytes);

    struct timespec timeStart;
    struct timespec timeEnd;
    unsigned long long diffSecs;
    unsigned long long diffMsecs;
    clock_gettime(CLOCK_MONOTONIC, &timeStart);

    while (true) {
        ssize_t numBytes = 0;
        ssize_t numBytesTotal = 0;
    tryAgain:
        if (work == Reader) {
            numBytes = read(fd,
                            (void *) ((uintptr_t) buffer + numBytesTotal),
                            ioSizeInBytes - numBytesTotal);
        } else {
            numBytes = write(fd,
                             (void *) ((uintptr_t) buffer + numBytesTotal),
                             ioSizeInBytes - numBytesTotal);
        }
        if (numBytes != (ssize_t) ioSizeInBytes) {
            if (numBytes != -1) {
                if (numBytes == 0) {
                    break;
                }
                numBytesTotal += numBytes;
                if (numBytesTotal < (ssize_t) ioSizeInBytes) {
                    goto tryAgain;
                }
            } else {
                status = sysErrnoToStatus(errno);
                printf("Failed iter %lu %s buffer size %lu: %s",
                       curIters,
                       work == Reader ? "Reads" : "Writes",
                       ioSizeInBytes,
                       strGetFromStatus(status));
                exit(-1);
            }
        }

        curIters++;
        totalBytes += ioSizeInBytes;

        if (verboseFlag) {
            printf("Thread %u %s Iter %lu totalBytes %ld\n",
                   threadNum,
                   work == Reader ? "Reads" : "Writes",
                   curIters,
                   totalBytes);
        }

        if (numIters > 0 && curIters == numIters) {
            break;
        }
    }
    free(buffer);
    buffer = NULL;

    if (-1 == close(fd)) {
        status = sysErrnoToStatus(errno);
        printf("Failed close %s: %s", ioPathCur, strGetFromStatus(status));
        exit(-1);
    }

    clock_gettime(CLOCK_MONOTONIC, &timeEnd);
    diffSecs = timeEnd.tv_sec - timeStart.tv_sec;
    diffMsecs =
        ((diffSecs * NSecsPerSec) + timeEnd.tv_nsec - timeStart.tv_nsec) /
        NSecsPerMSec;

    if (verboseFlag) {
        printf("Thread %u %s Iter %lu totalBytes %ld Elapsed time %llu\n",
               threadNum,
               work == Reader ? "Reads" : "Writes",
               curIters,
               totalBytes,
               diffMsecs);
    }

    tArgs->ioBytesDone = totalBytes;
    tArgs->iters = curIters;

    return NULL;
}

void
ioBenchmark()
{
    pthread_t *threads = (pthread_t *) malloc(sizeof(pthread_t) * numThreads);
    if (!threads) {
        printf("Failed to allocate buffer size %lu",
               sizeof(pthread_t) * numThreads);
        exit(-1);
    }
    ThreadArg *tArgs = (ThreadArg *) malloc(sizeof(ThreadArg) * numThreads);
    if (!tArgs) {
        printf("Failed to allocate buffer size %lu",
               sizeof(ThreadArg) * numThreads);
        exit(-1);
    }
    pthread_attr_t attr;

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    struct timespec timeStart;
    struct timespec timeEnd;
    unsigned long long diffSecs;
    unsigned long long diffMsecs;
    clock_gettime(CLOCK_MONOTONIC, &timeStart);

    memZero(tArgs, sizeof(ThreadArg) * numThreads);
    for (unsigned jj = 0; jj < numThreads; jj++) {
        tArgs[jj].threadNum = jj;
        tArgs[jj].work = Writer;
        // @SymbolCheckIgnore
        pthread_create(&threads[jj], &attr, ioThread, (void *) &tArgs[jj]);
    }

    for (unsigned jj = 0; jj < numThreads; jj++) {
        // @SymbolCheckIgnore
        pthread_join(threads[jj], NULL);
        ioBytesDoneWrite += tArgs[jj].ioBytesDone;
        itersWrite += tArgs[jj].iters;
    }

    clock_gettime(CLOCK_MONOTONIC, &timeEnd);
    diffSecs = timeEnd.tv_sec - timeStart.tv_sec;
    diffMsecs =
        ((diffSecs * NSecsPerSec) + timeEnd.tv_nsec - timeStart.tv_nsec) /
        NSecsPerMSec;

    if (diffMsecs && verboseFlag) {
        printf("Writers: Iters %u totalBytes %ld Elapsed time msec %llu\n",
               itersWrite,
               ioBytesDoneWrite,
               diffMsecs);
    }

    if (diffMsecs < 1000) {
        printf("Number of IO writes %u too small", numIters);
    } else {
        printf("IO Write throughput in bytes/sec: %llu, IOPS: %llu\n",
               ioBytesDoneWrite / (diffMsecs / 1000),
               itersWrite / (diffMsecs / 1000));
    }

    memZero(tArgs, sizeof(ThreadArg) * numThreads);
    for (unsigned jj = 0; jj < numThreads; jj++) {
        tArgs[jj].threadNum = jj;
        tArgs[jj].work = Reader;
        // @SymbolCheckIgnore
        pthread_create(&threads[jj], &attr, ioThread, (void *) &tArgs[jj]);
    }
    free(threads);
    threads = NULL;
    free(tArgs);
    tArgs = NULL;

    for (unsigned jj = 0; jj < numThreads; jj++) {
        // @SymbolCheckIgnore
        pthread_join(threads[jj], NULL);
        ioBytesDoneRead += tArgs[jj].ioBytesDone;
        itersRead += tArgs[jj].iters;
    }

    clock_gettime(CLOCK_MONOTONIC, &timeEnd);
    diffSecs = timeEnd.tv_sec - timeStart.tv_sec;
    diffMsecs =
        ((diffSecs * NSecsPerSec) + timeEnd.tv_nsec - timeStart.tv_nsec) /
        NSecsPerMSec;

    if (diffMsecs && verboseFlag) {
        printf("Readers: Iters %u totalBytes %ld Elapsed time msec %llu\n",
               itersRead,
               ioBytesDoneRead,
               diffMsecs);
    }

    if (diffMsecs < 1000) {
        printf("Number of IO reads %u too small", numIters);
    } else {
        printf("IO Read throughput in bytes/sec: %llu, IOPS: %llu\n",
               ioBytesDoneRead / (diffMsecs / 1000),
               itersRead / (diffMsecs / 1000));
    }
}

void
validateInput()
{
    if (helpFlag) {
        printHelp();
        exit(-1);
    }

    if (!ioSizeInBytes) {
        printf("Input 'ioSizeInBytes' required, not set!");
        printHelp();
        exit(-1);
    }

    if (!numThreads) {
        printf("Input 'numThreads' required, not set!");
        printHelp();
        exit(-1);
    }

    if (!numIters) {
        printf("Input 'numIters' not set!");
        printHelp();
        exit(-1);
    }

    if (ioPath[0] == '\0') {
        printf("Input 'ioPath' required, not set!");
        printHelp();
        exit(-1);
    }
}

static const char *long_options_help[] = {
    "--verbose",
    "--ioSizeInBytes (-i)   <IO size in Bytes>",
    "--numThreads (-n)      <Number of IO threads>",
    "--numIters (-t)        <Number of iterations>",
    "--help (-h)",
};

static void
printHelp(void)
{
    printf("\nioBenchmark\n");
    for (uint32_t ii = 0; ii < ArrayLen(long_options_help); ii++) {
        // @SymbolCheckIgnore
        printf("%s\n", long_options_help[ii]);
    }
}

int
main(int argc, char *argv[])
{
    int c;
    Status status;

    while (true) {
        static struct option long_options[] =
            {/* These options set a flag. */
             {"verbose", no_argument, &verboseFlag, 1},
             {"help", no_argument, &helpFlag, 1},
             {"ioSizeInBytes", required_argument, 0, 'i'},
             {"numThreads", required_argument, 0, 'n'},
             {"numIters", required_argument, 0, 't'},
             {"ioPath", required_argument, 0, 'f'},
             {0, 0, 0, 0}};

        /* getopt_long stores the option index here. */
        int option_index = 0;

        c = getopt_long(argc, argv, "i:n:d:t:f:", long_options, &option_index);

        /* Detect the end of the options. */
        if (c == -1) break;

        switch (c) {
        case 0:
            /* If this option set a flag, do nothing else now. */
            if (long_options[option_index].flag != 0) {
                break;
            }
            printf("Option %s\n", long_options[option_index].name);
            if (optarg) {
                printf(" with arg %s", optarg);
            }
            printf("\n");
            break;

        case 'i':
            ioSizeInBytes = atoll(optarg);
            if (!ioSizeInBytes) {
                printf("Invalid ioSizeInBytes");
                printHelp();
                exit(-1);
            }
            printf("Input: ioSizeInBytes: %lu\n", ioSizeInBytes);
            break;

        case 'n':
            numThreads = atoi(optarg);
            if (!numThreads) {
                printf("Invalid numThreads");
                printHelp();
                exit(-1);
            }
            printf("Input: numThreads: %u\n", numThreads);
            break;

        case 't':
            numIters = atoi(optarg);
            if (!numIters) {
                printf("Invalid numIters");
                printHelp();
                exit(-1);
            }
            printf("Input: numIters: %u\n", numIters);
            break;

        case 'f':
            status = strStrlcpy(ioPath, optarg, IOBUFSIZE);
            if (status != StatusOk) {
                printf("Invalid ioPath %s: %s",
                       optarg,
                       strGetFromStatus(status));
                printHelp();
                exit(-1);
            }
            printf("Input: ioPath: %s\n", ioPath);
            break;

        case '?':
            /* getopt_long already printed an error message. */
            break;

        default:
            printf("Invalid option: '%c'", c);
            printHelp();
            exit(-1);
        }
    }

    validateInput();

    ioBenchmark();

    return 0;
}
