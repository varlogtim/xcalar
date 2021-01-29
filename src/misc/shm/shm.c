#include <stdlib.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <sys/ipc.h>
#include <sys/shm.h>

size_t shmSize = 1024 * 1024 * 1024;
int numChild = 3;

void
setCgroup(const char *cgroupNameRoot)
{
    char *username = getenv("USER");
    char cgroupPath[512];
    pid_t pid = getpid();

    sprintf(cgroupPath,
            "/sys/fs/cgroup/memory/%s_%s/cgroup.procs",
            cgroupNameRoot,
            username);

    FILE *fp = fopen(cgroupPath, "w");
    fprintf(fp, "%d\n", pid);
    fclose(fp);
}

void
useShm()
{
    char c;
    int fd = shm_open("testShm", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

    ftruncate(fd, shmSize);

    char *foo =
        (char *) mmap(NULL, shmSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

    if (foo == NULL) {
        printf("foo is NULL");
        return;
    }
    mlock(foo, shmSize);

    for (int ii = 0; ii < numChild; ii++) {
        if (fork() == 0) {
            setCgroup("xcalar_sys_xpus");
            for (size_t jj = 0; jj < shmSize; jj++) {
                foo[jj] = '\x01';
            }
            printf("I'm the kid. Type something here\n");
            scanf("%s", (char *) foo);
            return;
        }
    }
    wait(NULL);
    printf("I am the parent: %s\n", foo);
    printf("Press enter to continue\n");
    scanf("%s", foo);
}

void
useShmGet()
{
    char c;
    int shmid = shmget(IPC_PRIVATE, shmSize, SHM_R | SHM_W);
    printf("shmid is %d\n", shmid);

    char *foo = (char *) shmat(shmid, 0, 0);
    if (foo == NULL) {
        printf("foo is NULL");
        return;
    }
    mlock(foo, shmSize);

    for (int ii = 0; ii < numChild; ii++) {
        if (fork() == 0) {
            setCgroup("xcalar_sys_xpus");
            if (int err = shmdt(foo) != 0) {
                printf("Error %d while detaching foo\n", err);
                return;
            }
            char *foo2 = (char *) shmat(shmid, 0, 0);
            for (size_t jj = 0; jj < shmSize; jj++) {
                foo2[jj] = '\x01';
            }
            printf("I'm the kid. Type something here\n");
            scanf("%s", (char *) foo2);
            return;
        }
    }
    wait(NULL);
    printf("I am the parent: %s\n", foo);
    printf("Press enter to continue\n");
    scanf("%s", foo);
}

void
useMmap()
{
    char c;
    char *foo = (char *) mmap(NULL,
                              shmSize,
                              PROT_READ | PROT_WRITE,
                              MAP_SHARED | MAP_ANONYMOUS,
                              -1,
                              0);
    if (foo == NULL) {
        printf("foo is NULL");
        return;
    }
    mlock(foo, shmSize);

    for (int ii = 0; ii < numChild; ii++) {
        if (fork() == 0) {
            setCgroup("xcalar_sys_xpus");
            for (size_t jj = 0; jj < shmSize; jj++) {
                foo[jj] = '\x01';
            }
            printf("I'm the kid. Type something here\n");
            scanf("%s", (char *) foo);
            return;
        }
    }
    wait(NULL);
    printf("I am the parent: %s\n", foo);

    printf("Press enter to continue\n");
    scanf("%s", foo);
}

int
main(int argc, char *argv[])
{
    int choice;

    setCgroup("xcalar_xce");
    printf("Enter 1 for shm_open, 2 for shmget, and 3 for mmap\n");
    scanf("%d", &choice);
    if (choice == 1) {
        useShm();
    } else if (choice == 2) {
        useShmGet();
    } else {
        useMmap();
    }

    return (0);
}
