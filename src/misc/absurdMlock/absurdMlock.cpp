#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/mman.h>

uint64_t KB = 1024;
uint64_t MB = KB * 1024;
uint64_t GB = MB * 1024;

int main(int argc, char *argv[]) {
    uint64_t ii, jj;
    size_t bufSize = 1 * GB;
    size_t numBufs = 10 * KB;
    // @SymbolCheckIgnore
    uint8_t **buf = (uint8_t **) malloc(sizeof(*buf) * numBufs);
    size_t physicalRam;
    size_t lockedRamSize;
    uint8_t *lockedRam;

    physicalRam = sysconf(_SC_PHYS_PAGES) * sysconf(_SC_PAGESIZE);
    lockedRamSize = (physicalRam * 6) / 10;
    printf("Ram: %lu bytes, Locked Ram: %lu bytes\n", physicalRam,
           lockedRamSize);

    // @SymbolCheckIgnore
    lockedRam = (uint8_t *) mmap(NULL, lockedRamSize, PROT_READ | PROT_WRITE,
                                 MAP_ANONYMOUS | MAP_PRIVATE | MAP_NORESERVE,
                                 -1, 0);
    if (lockedRam == (void *) -1) {
        perror("lockedRam is NULL");
        // @SymbolCheckIgnore
        exit(1);
    }

    printf("lockedRam: %p, lockedRamSize: %lu bytes, lockedRamEnd: %p\n",
           lockedRam, lockedRamSize, lockedRam + lockedRamSize);
    int ret = mlock(lockedRam, lockedRamSize);
    if (ret != 0) {
        perror("Could not mlock");
        // @SymbolCheckIgnore
        exit(1);
    }
    printf("mlock is done\n");
    printf("All malloc succeeded. Press enter to start touching memory\n");
    int sz = scanf("%lu", &jj);
    ((void)sz);

    for (jj = 0; jj < numBufs; jj++) {
        for (ii = 0; ii < bufSize; ii++) {
            buf[jj][ii] = '1';
        }
    }

    for (jj = 0; jj < numBufs; jj++) {
        buf[jj] = (uint8_t *) malloc(bufSize);
        if (buf[jj] == NULL) {
            printf("Buf[%lu] is NULL", jj);
            // @SymbolCheckIgnore
            exit(1);
        }
    }


    return 0;
}
