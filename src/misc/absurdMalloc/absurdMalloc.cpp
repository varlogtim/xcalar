#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>

uint64_t KB = 1024;
uint64_t MB = KB * 1024;
uint64_t GB = MB * 1024;

int main(int argc, char *argv[]) {
    uint64_t ii, jj;
    char *notUsed;
    size_t bufSize = 1 * GB;
    size_t numBufs = 10 * KB;
    size_t len;
    // @SymbolCheckIgnore
    uint8_t **buf = (uint8_t **) malloc(sizeof(*buf) * numBufs);

    for (jj = 0; jj < numBufs; jj++) {
        // @SymbolCheckIgnore
        buf[jj] = (uint8_t *) malloc(bufSize);
        if (buf[jj] == NULL) {
            printf("Buf[%lu] is NULL", jj);
            // @SymbolCheckIgnore
            exit(1);
        }
    }

    if (isatty(fileno(stdin))) {
        printf("All malloc succeeded. Press enter to start touching memory\n");
        ssize_t sz = getline(&notUsed, &len, stdin);
        ((void)sz);
    }

    for (jj = 0; jj < numBufs; jj++) {
        for (ii = 0; ii < bufSize; ii++) {
            buf[jj][ii] = '1';
        }
    }

    return 0;
}
