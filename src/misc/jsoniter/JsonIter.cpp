// Copyright 2014 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

// iterate through a json file printing all objects' key:value pairs using
// libjansson

// reference: https://jansson.readthedocs.org/en/latest/apiref.html

#include <cstdlib>
#include <string.h>
#include <assert.h>
#include <stdbool.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <err.h>

#include <jansson.h>

#include "primitives/Primitives.h"
#include "util/System.h"

// set shouldPrint == false when benchmarking iteration speed
static bool shouldPrint = true;
static unsigned objCount = 0;

static void iterate(json_t *jsonObj, unsigned depth, bool atNewLine);
static void indent(unsigned depth, bool atNewLine);

int
main(int argc, char *argv[])
{
    json_t *root;
    json_error_t error;

    if (argc != 2) {
        fprintf(stderr, "usage: %s <jsonFile>\n\n", argv[0]);
        return 1;
    }

    struct stat statbuf;
    int ret = stat(argv[1], &statbuf);
    assert(ret == 0);

    // @SymbolCheckIgnore
    uint8_t *buf = (uint8_t *) malloc(statbuf.st_size);
    assert(buf);
    int fd = open(argv[1], O_RDONLY);
    if (fd < 0) {
        err(1, "Failed to open %s", argv[1]);
    }

    size_t totBytesRead = 0;
    do {
        ssize_t bytesRead = read(fd, &buf[totBytesRead],
                                 statbuf.st_size - totBytesRead);
        if (bytesRead <= 0) {
            err(1, "Failed to read %s", argv[1]);
        }
        totBytesRead += bytesRead;
    } while (totBytesRead < (size_t) statbuf.st_size);

    struct timespec startTime, endTime;

    ret = clock_gettime(CLOCK_REALTIME, &startTime);
    assert(ret == 0);

    root = json_loadb((char *) buf, statbuf.st_size, 0, &error);
    if (!root) {
        fprintf(stderr, "error: on line %d: %s\n", error.line, error.text);
        return 1;
    }

    ret = clock_gettime(CLOCK_REALTIME, &endTime);
    assert(ret == 0);

    double durationInSeconds =
        (double) (clkGetElapsedTimeInNanosSafe(&startTime, &endTime)) /
        NSecsPerSec;
    double fileSizeInMB = ((double) statbuf.st_size) / MB;
    double rate = fileSizeInMB / durationInSeconds;

    fprintf(stderr, "Parsed %g MB json in %g s (%g MB/s)\n", fileSizeInMB,
            durationInSeconds, rate);

    ret = clock_gettime(CLOCK_REALTIME, &startTime);
    assert(ret == 0);

    iterate(root, 0, true);

    ret = clock_gettime(CLOCK_REALTIME, &endTime);
    assert(ret == 0);

    double durationInMSeconds =
        (double) (clkGetElapsedTimeInNanosSafe(&startTime, &endTime)) /
        NSecsPerMSec;
    rate = fileSizeInMB / durationInMSeconds;

    fprintf(stderr, "Iterate %g MB json in %g ms (%g MB/ms) %u objs\n",
            fileSizeInMB,
            durationInMSeconds, rate, objCount);

    // @SymbolCheckIgnore
    free(buf);
    close(fd);

    return 0;
}

#define PRINT(...) if (shouldPrint) printf(__VA_ARGS__)

void
iterate(json_t *json, unsigned depth, bool atNewLine)
{
    const char *key;
    json_t *value;
    unsigned ii;
    void *iter;

    switch (json_typeof(json)) {
    case JSON_OBJECT:
        objCount++;
        iter = json_object_iter(json);

        indent(depth, atNewLine);

        PRINT("{");
        while (iter) {
            PRINT("\n");
            key = json_object_iter_key(iter);
            value = json_object_iter_value(iter);

            /* use key and value ... */
            indent(depth + 1, true);
            PRINT("\"%s\":", key);
            iterate(value, depth + 1, false);

            iter = json_object_iter_next(json, iter);
            if (iter != NULL) {
                PRINT(",");
            }
        }

        PRINT("\n");
        indent(depth, true);
        PRINT("}");

        break;

    case JSON_ARRAY:
        indent(depth, atNewLine);

        for (ii = 0; ii < json_array_size(json); ii++) {
            PRINT("[\n");
            value = json_array_get(json, ii);
            iterate(value, depth + 1, true);
            PRINT("\n");
            indent(depth, true);
            PRINT("]");
        }
        break;

    case JSON_STRING:
        PRINT("\"%s\"", json_string_value(json));
        break;
    case JSON_INTEGER:
        PRINT("%d", (int) json_integer_value(json));
        break;
    case JSON_REAL:
        PRINT("%g", json_real_value(json));
        break;
    case JSON_TRUE:
        PRINT("true");
        break;
    case JSON_FALSE:
        PRINT("true");
        break;
    case JSON_NULL:
        PRINT("<null>");
        break;
    default:
        assert(0);
        fprintf(stderr, "don't yet know how to iterate %u\n",
                json_typeof(json));
    }
}

void
indent(unsigned depth, bool atNewLine)
{
    unsigned ii;

    if (atNewLine) {
        for (ii = 0; ii < depth * 2; ii++) {
            PRINT(" ");
        }
    }
}
