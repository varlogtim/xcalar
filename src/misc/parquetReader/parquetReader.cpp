// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "df/DataFormat.h"
#include "dataformat/DataFormatParquet.h"

int
main(int argc, char *argv[])
{
    int fd;
    struct stat st;
    int ret;
    uint8_t *data = NULL;
    ParquetParser parser;
    Status status;

    if (argc < 2) {
        fprintf(stderr, "Usage: %s <path-to-parquet-file>\n", argv[0]);
        return 0;
    }

    fd = open(argv[1], O_RDONLY);
    if (fd < 0) {
        perror("Could not open file");
        return 1;
    }

    ret = fstat(fd, &st);
    if (ret < 0) {
        perror("Error getting file size");
        return 1;
    }

    data = (uint8_t *) malloc(st.st_size);
    if (data == NULL) {
        fprintf(stderr, "Could not malloc %lu of memory", st.st_size);
        return 1;
    }

    ret = read(fd, data, st.st_size);
    if (ret < 0) {
        perror("Could not read file");
        return 1;
    }

    status = parser.getSchema(data, st.st_size);
    return 0;
}
