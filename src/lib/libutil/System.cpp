// Copyright 2013-2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <sys/time.h>
#include <sys/resource.h>
#include <sys/sysinfo.h>
#include <unistd.h>
#include <stdio.h>
#include <new>
#include <execinfo.h>
#include <stdlib.h>
#include <regex.h>
#include <dlfcn.h>
#include <cxxabi.h>
#include <errno.h>
#include <netdb.h>
#include <time.h>

#include "primitives/Primitives.h"
#include "util/System.h"
#include "operators/GenericTypes.h"
#include "config/Config.h"
#include "stat/Statistics.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"

#define MAXERRNO EHWPOISON
#define MAXSTATUSCODE StatusCodeHwPoison

static void sleepCommon(const struct timespec *timeout);

static constexpr const char *moduleName = "libutilSystem";

StatusCode
sysErrnoToStatusCode(int errn)
{
    StatusCode sts;

    assertStatic(EPERM == StatusCodePerm);
    assertStatic(ENOMEM == StatusCodeNoMem);
    assert(errn >= 0 && errn <= MAXERRNO);

    // no way to ensure the above at runtime on new production systems, so
    // we also need a runtime check
    if (errn < 0 || errn > MAXERRNO) {
        return StatusCodeUnknown;
    }

    switch (errn) {
    case 0:
        sts = StatusCodeOk;
        break;
        // override known errnos
    case EPERM:
        sts = StatusCodePerm;
        break;
    case ENOENT:
        sts = StatusCodeNoEnt;
        break;
    case ESRCH:
        sts = StatusCodeSrch;
        break;
    case EINTR:
        sts = StatusCodeIntr;
        break;
    case EIO:
        sts = StatusCodeIO;
        break;
    case ENXIO:
        sts = StatusCodeNxIO;
        break;
    case E2BIG:
        sts = StatusCode2Big;
        break;
    case ENOEXEC:
        sts = StatusCodeNoExec;
        break;
    case EBADF:
        sts = StatusCodeBadF;
        break;
    case ECHILD:
        sts = StatusCodeChild;
        break;
    case EAGAIN:
        sts = StatusCodeAgain;
        break;
    case ENOMEM:
        sts = StatusCodeNoMem;
        break;
    case EACCES:
        sts = StatusCodeAccess;
        break;
    case EFAULT:
        sts = StatusCodeFault;
        break;
    case ENOTBLK:
        sts = StatusCodeNotBlk;
        break;
    case EBUSY:
        sts = StatusCodeBusy;
        break;
    case EEXIST:
        sts = StatusCodeExist;
        break;
    case EXDEV:
        sts = StatusCodeXDev;
        break;
    case ENODEV:
        sts = StatusCodeNoDev;
        break;
    case ENOTDIR:
        sts = StatusCodeNotDir;
        break;
    case EISDIR:
        sts = StatusCodeIsDir;
        break;
    case EINVAL:
        sts = StatusCodeInval;
        break;
    case ENFILE:
        sts = StatusCodeNFile;
        break;
    case EMFILE:
        sts = StatusCodeMFile;
        break;
    case ENOTTY:
        sts = StatusCodeNoTTY;
        break;
    case ETXTBSY:
        sts = StatusCodeTxtBsy;
        break;
    case EFBIG:
        sts = StatusCodeFBig;
        break;
    case ENOSPC:
        sts = StatusCodeNoSpc;
        break;
    case ESPIPE:
        sts = StatusCodeSPipe;
        break;
    case EROFS:
        sts = StatusCodeROFS;
        break;
    case EMLINK:
        sts = StatusCodeMLink;
        break;
    case EPIPE:
        sts = StatusCodePipe;
        break;
    case EDOM:
        sts = StatusCodeDom;
        break;
    case ERANGE:
        sts = StatusCodeRange;
        break;
    case EDEADLK:
        sts = StatusCodeDeadLk;
        break;
    case ENAMETOOLONG:
        sts = StatusCodeNameTooLong;
        break;
    case ENOLCK:
        sts = StatusCodeNoLck;
        break;
    case ENOSYS:
        sts = StatusCodeNoSys;
        break;
    case ENOTEMPTY:
        sts = StatusCodeNotEmpty;
        break;
    case ELOOP:
        sts = StatusCodeLoop;
        break;
    case ENOMSG:
        sts = StatusCodeNoMsg;
        break;
    case EIDRM:
        sts = StatusCodeIdRm;
        break;
    case ECHRNG:
        sts = StatusCodeChRng;
        break;
    case EL2NSYNC:
        sts = StatusCodeL2NSync;
        break;
    case EL3HLT:
        sts = StatusCodeL3Hlt;
        break;
    case EL3RST:
        sts = StatusCodeL3Rst;
        break;
    case ELNRNG:
        sts = StatusCodeLNRng;
        break;
    case EUNATCH:
        sts = StatusCodeUnatch;
        break;
    case ENOCSI:
        sts = StatusCodeNoCSI;
        break;
    case EL2HLT:
        sts = StatusCodeL2Hlt;
        break;
    case EBADE:
        sts = StatusCodeBadE;
        break;
    case EBADR:
        sts = StatusCodeBadR;
        break;
    case EXFULL:
        sts = StatusCodeXFull;
        break;
    case ENOANO:
        sts = StatusCodeNoAno;
        break;
    case EBADRQC:
        sts = StatusCodeBadRqC;
        break;
    case EBADSLT:
        sts = StatusCodeBadSlt;
        break;
    case EBFONT:
        sts = StatusCodeBFont;
        break;
    case ENOSTR:
        sts = StatusCodeNoStr;
        break;
    case ENODATA:
        sts = StatusCodeNoData;
        break;
    case ETIME:
        sts = StatusCodeTime;
        break;
    case ENOSR:
        sts = StatusCodeNoSR;
        break;
    case ENONET:
        sts = StatusCodeNoNet;
        break;
    case ENOPKG:
        sts = StatusCodeNoPkg;
        break;
    case EREMOTE:
        sts = StatusCodeRemote;
        break;
    case ENOLINK:
        sts = StatusCodeNoLink;
        break;
    case EADV:
        sts = StatusCodeAdv;
        break;
    case ESRMNT:
        sts = StatusCodeSRMnt;
        break;
    case ECOMM:
        sts = StatusCodeComm;
        break;
    case EPROTO:
        sts = StatusCodeProto;
        break;
    case EMULTIHOP:
        sts = StatusCodeMultihop;
        break;
    case EDOTDOT:
        sts = StatusCodeDotDot;
        break;
    case EBADMSG:
        sts = StatusCodeBadMsg;
        break;
    case EOVERFLOW:
        sts = StatusCodeOverflow;
        break;
    case ENOTUNIQ:
        sts = StatusCodeNotUniq;
        break;
    case EBADFD:
        sts = StatusCodeBadFD;
        break;
    case EREMCHG:
        sts = StatusCodeRemChg;
        break;
    case ELIBACC:
        sts = StatusCodeLibAcc;
        break;
    case ELIBBAD:
        sts = StatusCodeLibBad;
        break;
    case ELIBSCN:
        sts = StatusCodeLibScn;
        break;
    case ELIBMAX:
        sts = StatusCodeLibMax;
        break;
    case ELIBEXEC:
        sts = StatusCodeLibExec;
        break;
    case EILSEQ:
        sts = StatusCodeIlSeq;
        break;
    case ERESTART:
        sts = StatusCodeRestart;
        break;
    case ESTRPIPE:
        sts = StatusCodeStrPipe;
        break;
    case EUSERS:
        sts = StatusCodeUsers;
        break;
    case ENOTSOCK:
        sts = StatusCodeNotSock;
        assert(0);
        break;
    case EDESTADDRREQ:
        sts = StatusCodeDestAddrReq;
        break;
    case EMSGSIZE:
        sts = StatusCodeMsgSize;
        break;
    case EPROTOTYPE:
        sts = StatusCodePrototype;
        break;
    case ENOPROTOOPT:
        sts = StatusCodeNoProtoOpt;
        break;
    case EPROTONOSUPPORT:
        sts = StatusCodeProtoNoSupport;
        break;
    case ESOCKTNOSUPPORT:
        sts = StatusCodeSockTNoSupport;
        break;
    case EOPNOTSUPP:
        sts = StatusCodeOpNotSupp;
        break;
    case EPFNOSUPPORT:
        sts = StatusCodePFNoSupport;
        break;
    case EAFNOSUPPORT:
        sts = StatusCodeAFNoSupport;
        break;
    case EADDRINUSE:
        sts = StatusCodeAddrInUse;
        break;
    case EADDRNOTAVAIL:
        sts = StatusCodeAddrNotAvail;
        break;
    case ENETDOWN:
        sts = StatusCodeNetDown;
        break;
    case ENETUNREACH:
        sts = StatusCodeNetUnreach;
        break;
    case ENETRESET:
        sts = StatusCodeNetReset;
        break;
    case ECONNABORTED:
        sts = StatusCodeConnAborted;
        break;
    case ECONNRESET:
        sts = StatusCodeConnReset;
        break;
    case ENOBUFS:
        sts = StatusCodeNoBufs;
        break;
    case EISCONN:
        sts = StatusCodeIsConn;
        break;
    case ENOTCONN:
        sts = StatusCodeNotConn;
        break;
    case ESHUTDOWN:
        sts = StatusCodeShutdown;
        break;
    case ETOOMANYREFS:
        sts = StatusCodeTooManyRefs;
        break;
    case ETIMEDOUT:
        sts = StatusCodeTimedOut;
        break;
    case ECONNREFUSED:
        sts = StatusCodeConnRefused;
        break;
    case EHOSTDOWN:
        sts = StatusCodeHostDown;
        break;
    case EHOSTUNREACH:
        sts = StatusCodeHostUnreach;
        break;
    case EALREADY:
        sts = StatusCodeAlready;
        break;
    case EINPROGRESS:
        sts = StatusCodeInProgress;
        break;
    case ESTALE:
        sts = StatusCodeStale;
        break;
    case EUCLEAN:
        sts = StatusCodeUClean;
        break;
    case ENOTNAM:
        sts = StatusCodeNotNam;
        break;
    case ENAVAIL:
        sts = StatusCodeNAvail;
        break;
    case EISNAM:
        sts = StatusCodeIsNam;
        break;
    case EREMOTEIO:
        sts = StatusCodeRemoteIo;
        break;
    case EDQUOT:
        sts = StatusCodeDQuot;
        break;
    case ENOMEDIUM:
        sts = StatusCodeNoMedium;
        break;
    case EMEDIUMTYPE:
        sts = StatusCodeMediumType;
        break;
    case ECANCELED:
        sts = StatusCodeCanceled;
        break;
    case ENOKEY:
        sts = StatusCodeNoKey;
        break;
    case EKEYEXPIRED:
        sts = StatusCodeKeyExpired;
        break;
    case EKEYREVOKED:
        sts = StatusCodeKeyRevoked;
        break;
    case EKEYREJECTED:
        sts = StatusCodeKeyRejected;
        break;
    case EOWNERDEAD:
        sts = StatusCodeOwnerDead;
        break;
    case ENOTRECOVERABLE:
        sts = StatusCodeNotRecoverable;
        break;
    case ERFKILL:
        sts = StatusCodeRFKill;
        break;
    case EHWPOISON:
        sts = StatusCodeHwPoison;
        break;
    default:
        sts = StatusCodeUnknown;
        break;
    }

    assert(sts <= MAXSTATUSCODE && (sts != StatusCodeOk || errn == 0) &&
           sts != StatusCodeUnknown);

    return sts;
}

Status
sysErrnoToStatus(int errn)
{
    StatusCode code = sysErrnoToStatusCode(errn);
    Status status;
    status.fromStatusCode(code);
    return status;
}

Status
sysEaiErrToStatus(int eaiErr)
{
    Status sts;

    switch (eaiErr) {
    case EAI_BADFLAGS:
        sts = StatusEAIBadFlags;
        break;
    case EAI_NONAME:
        sts = StatusEAINoName;
        break;
    case EAI_AGAIN:
        sts = StatusAgain;
        break;
    case EAI_FAIL:
        sts = StatusEAIFail;
        break;
    case EAI_FAMILY:
        sts = StatusProtoNoSupport;
        break;
    case EAI_SOCKTYPE:
        sts = StatusSockTNoSupport;
        break;
    case EAI_SERVICE:
        sts = StatusEAIService;
        break;
    case EAI_MEMORY:
        sts = StatusNoMem;
        break;
    case EAI_SYSTEM:
        sts = sysErrnoToStatus(errno);
        break;
    case EAI_OVERFLOW:
        sts = StatusOverflow;
        break;
    case EAI_NODATA:
        sts = StatusEAINoData;
        break;
    case EAI_ADDRFAMILY:
        sts = StatusEAIAddrFamily;
        break;
    case EAI_INPROGRESS:
        sts = StatusInProgress;
        break;
    case EAI_CANCELED:
        sts = StatusCanceled;
        break;
    case EAI_NOTCANCELED:
        sts = StatusEAINotCancel;
        break;
    case EAI_ALLDONE:
        sts = StatusEAIAllDone;
        break;
    case EAI_INTR:
        sts = StatusIntr;
        break;
    case EAI_IDN_ENCODE:
        sts = StatusEAIIDNEncode;
        break;
    default:
        assert(0);
        sts = StatusUnknown;
        break;
    }

    return sts;
}

void
sysSleep(uint32_t numSecs)
{
    struct timespec timeout;

    timeout.tv_sec = numSecs;
    timeout.tv_nsec = 0;

    sleepCommon(&timeout);
}

void
sysUSleep(uint64_t numUSecs)
{
    struct timespec timeout;

    timeout.tv_sec = numUSecs / USecsPerSec;
    timeout.tv_nsec = (numUSecs % USecsPerSec) * NSecsPerUSec;
    if ((unsigned long) timeout.tv_nsec > NSecsPerSec) {
        timeout.tv_sec++;
        timeout.tv_nsec -= (int64_t) NSecsPerSec;
    }

    sleepCommon(&timeout);
}

void
sysNSleep(uint64_t numNSecs)
{
    struct timespec timeout;
    timeout.tv_sec = numNSecs / NSecsPerSec;
    timeout.tv_nsec = (numNSecs % NSecsPerSec);

    sleepCommon(&timeout);
}

void
sleepCommon(const struct timespec *timeout)
{
    int ret;
    struct timespec tsIn, tsOut;
    unsigned count = 0;

    tsIn = *timeout;

    do {
        // @SymbolCheckIgnore
        ret = clock_nanosleep(CLOCK_REALTIME, 0, &tsIn, &tsOut);
        tsIn = tsOut;
        assert(count++ < 1000);
    } while (ret == EINTR);
}

Status
sysCondTimedWait(pthread_cond_t *cond,
                 pthread_mutex_t *mutex,
                 uint64_t timeoutInUSec)
{
    int ret;
    struct timespec absTime;

    ret = clock_gettime(CLOCK_REALTIME, &absTime);
    if (ret != 0) {
        return sysErrnoToStatus(errno);
    }
    absTime.tv_sec += timeoutInUSec / USecsPerSec;
    absTime.tv_nsec += (timeoutInUSec % USecsPerSec) * NSecsPerUSec;
    if ((unsigned long) absTime.tv_nsec >= NSecsPerSec) {
        absTime.tv_sec++;
        absTime.tv_nsec -= NSecsPerSec;
    }

    assert(absTime.tv_nsec >= 0);
    assert((unsigned long) absTime.tv_nsec < NSecsPerSec);

    do {
        // @SymbolCheckIgnore
        ret = pthread_cond_timedwait(cond, mutex, (struct timespec *) &absTime);
    } while (ret == EINTR);

    return sysErrnoToStatus(ret);
}

Status
sysSemTimedWait(sem_t *sem, uint64_t timeoutInUSec)
{
    int ret;
    struct timespec absTime;

    ret = clock_gettime(CLOCK_REALTIME, &absTime);
    if (ret != 0) {
        return sysErrnoToStatus(errno);
    }
    absTime.tv_sec += timeoutInUSec / USecsPerSec;
    absTime.tv_nsec += (timeoutInUSec % USecsPerSec) * NSecsPerUSec;
    if ((unsigned long) absTime.tv_nsec >= NSecsPerSec) {
        absTime.tv_sec++;
        absTime.tv_nsec -= NSecsPerSec;
    }

    assert(absTime.tv_nsec >= 0);
    assert((unsigned long) absTime.tv_nsec < NSecsPerSec);

    do {
        // @SymbolCheckIgnore
        ret = sem_timedwait(sem, (struct timespec *) &absTime);
        if (ret < 0) {
            ret = errno;
        }
    } while (ret == EINTR);

    return sysErrnoToStatus(ret);
}

void
// @SymbolCheckIgnore
panic(const char *fmt, ...)
{
    va_list argptr;

    va_start(argptr, fmt);
    xSyslogV(moduleName,
             XlogEmerg,
             TxnBufNoCopy,
             NotFromSignalHandler,
             fmt,
             argptr);
    va_end(argptr);

    xSyslogFlush();

    __builtin_trap();
}

void
// @SymbolCheckIgnore
panicNoCore(const char *fmt, ...)
{
    va_list argptr;

    va_start(argptr, fmt);
    xSyslogV(moduleName,
             XlogEmerg,
             TxnBufNoCopy,
             NotFromSignalHandler,
             fmt,
             argptr);
    va_end(argptr);

    xcalarExit(1);
}

ssize_t
getBacktrace(void **buf, const ssize_t maxFrames)
{
    return backtrace(buf, maxFrames);
}

void
printBackTrace(char *destBuf, int destBufSize, int numFrames, void **frames)
{
    regex_t preg;
    bool regInited = false;
    const char *pattern = ".*\\(([a-zA-Z0-9_]+)\\+(0x[0-9a-z]+)\\)";
    char *demangledFuncName = NULL;
    int consumed = 0;
    char **btSymbolList = NULL;
    int ret;

    snprintf(destBuf, destBufSize, "Stack trace unavailable");

    // XXX - it would be nice to only compile this once
    int rc = regcomp(&preg, pattern, REG_EXTENDED);
    if (rc != 0) {
        goto CommonExit;
    }
    regInited = true;

    btSymbolList = backtrace_symbols(frames, numFrames);
    if (btSymbolList == NULL) {
        goto CommonExit;
    }

    for (int ii = 1; consumed < destBufSize && ii < numFrames; ii++) {
        size_t nmatch = 3;
        regmatch_t pmatch[nmatch];

        rc = regexec(&preg, btSymbolList[ii], nmatch, pmatch, 0);
        if (rc == 0) {
            int fnStart = pmatch[1].rm_so;
            int fnEnd = pmatch[1].rm_eo;
            int fnLen = fnEnd - fnStart;
            int offStart = pmatch[2].rm_so;
            int offEnd = pmatch[2].rm_eo;
            int offLen = offEnd - offStart;

            const char *matchStr = &btSymbolList[ii][fnStart];
            const char *offStr = &btSymbolList[ii][offStart];
            char mangledSymbol[fnLen + 1];

            strlcpy(mangledSymbol, matchStr, sizeof(mangledSymbol));

            // This `malloc`s our demangled func name for us
            demangledFuncName =
                abi::__cxa_demangle(mangledSymbol, NULL, 0, &ret);
            const char *functionName =
                demangledFuncName ? demangledFuncName : mangledSymbol;
            ret = snprintf(destBuf + consumed,
                           destBufSize - consumed,
                           "# %d  %s + %.*s\n",
                           ii,
                           functionName,
                           offLen,
                           offStr);
            consumed += ret;
            // @SymbolCheckIgnore
            free(demangledFuncName);
            demangledFuncName = NULL;
        } else {
            ret = snprintf(destBuf + consumed,
                           destBufSize - consumed,
                           "# %d  %s\n",
                           ii,
                           btSymbolList[ii]);
            consumed += ret;
        }
    }
CommonExit:
    if (regInited) {
        regfree(&preg);
        regInited = false;
    }
    if (demangledFuncName) {
        // @SymbolCheckIgnore
        free(demangledFuncName);
        demangledFuncName = NULL;
    }
    if (btSymbolList) {
        // @SymbolCheckIgnore
        free(btSymbolList);
        btSymbolList = NULL;
    }
}

std::string
runCmdAndReturnStdout(std::string cmd, Status *retStatus)
{
    std::string data;
    FILE *stream;
    const int MaxBuffer = 256;
    char buffer[MaxBuffer];
    cmd.append(" 2>&1");
    *retStatus = StatusOk;

    stream = popen(cmd.c_str(), "r");
    if (stream) {
        while (!feof(stream)) {
            if (fgets(buffer, MaxBuffer, stream) != NULL) {
                data.append(buffer);
            } else if (ferror(stream)) {
                *retStatus = sysErrnoToStatus(errno);
                clearerr(stream);
                break;
            }
        }
        if (pclose(stream) < 0) {
            if (*retStatus == StatusOk) {
                *retStatus = sysErrnoToStatus(errno);
            }
            if (*retStatus == StatusChild) {
                // If a call caused the termination status to be unavailable
                // to pclose(), then pclose() returns -1 with errno set to
                // ECHILD to report this situation; this can happen if the
                // application calls one of the following functions:
                // * wait()
                // * waitid()
                // * waitpid() with a pid argument less than or equal to the
                //   process ID of the shell command any other function that
                //   could do one of the above.
                *retStatus = StatusOk;
            }
        }
    } else {
        *retStatus = StatusNoMem;
    }
    return data;
}

Status
runSystemCmd(std::string cmd)
{
    Status status = StatusOk;
    int ret = system(cmd.c_str());
    if (ret == -1) {
        status = sysErrnoToStatus(errno);
        if (status == StatusChild) {
            // If a call caused the termination status to be unavailable
            // to pclose(), then pclose() returns -1 with errno set to
            // ECHILD to report this situation; this can happen if the
            // application calls one of the following functions:
            // * wait()
            // * waitid()
            // * waitpid() with a pid argument less than or equal to the
            //   process ID of the shell command any other function that
            //   could do one of the above.
            status = StatusOk;
        }
    }
    return status;
}

bool
sysIsProcessCoreDumping(pid_t pid)
{
    FILE *fp = NULL;
    char cmdString[50];
    char line[1024];
    bool isCoreDumping = false;

    snprintf(cmdString,
             sizeof(cmdString),
             "cat /proc/%d/stat | cut -d\\  -f3",
             pid);
    fp = popen(cmdString, "r");
    isCoreDumping = ((fgets(line, sizeof(line), fp) != NULL) && line[0] == 'D');
    xSyslog(moduleName, XlogDebug, "%s: %s", cmdString, line);
    pclose(fp);

    return isCoreDumping;
}

static constexpr char ZeroPage[PageSize] = {0};
Status
sparseMemCpy(void *dstIn,
             const void *srcIn,
             size_t size,
             size_t blkSize,
             size_t *bytesPhysOut)
{
    char *dst = (char *) dstIn;
    const char *src = (const char *) srcIn;
    size_t bytesPhys = 0;

    if (blkSize > PageSize) {
        assert(false && "Invalid block size");
        xSyslog(moduleName,
                XlogErr,
                "Invalid block size %lu > %lu",
                blkSize,
                PageSize);
        return StatusInval;
    }

    while (size != 0 && size >= blkSize) {
        if (memcmp(src, ZeroPage, blkSize)) {
            memcpy(dst, src, blkSize);
            bytesPhys += blkSize;
        }
        size -= blkSize;
        src += blkSize;
        dst += blkSize;
    }

    if (size) {
        assert(size < blkSize);
        if (memcmp(src, ZeroPage, size)) {
            memcpy(dst, src, size);
            bytesPhys += size;
        }
    }

    if (bytesPhysOut) {
        (*bytesPhysOut) = bytesPhys;
    }
    return StatusOk;
}
