// Copyright 2018-2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _FSTATUS_H_
#define _FSTATUS_H_

#include <assert.h>
#include <stdio.h>

#include "primitives/StatusCode.h"
#include "primitives/Macros.h"
#include "StrlFunc.h"

// Note that Status is a struct now and needs to be kept as an Int variable bc
// it's used all over in the fast path to propagate error codes.
struct Status {
  public:
    static inline void copyStatus(Status *dst, const Status *src)
    {
        dst->status_ = src->status_;
    }

    Status() { status_ = StatusCodeOk; }

    // Copy constructor
    // Handles cases like:
    //     Status status2 = StatusOk;
    //     Status status = status2;
    Status(const Status &other) { copyStatus(this, &other); }

    // Move constructor
    // Handles cases like:
    //     Status status = someFunc();
    // https://docs.microsoft.com/en-us/cpp/cpp/move-constructors-and-move-assignment-operators-cpp?view=vs-2019
    // Refer above document on how to invoke Move assignment operator from Move
    // contructor.
    Status(Status &&other) { copyStatus(this, &other); }

    // Copy assignment operator
    // Handles cases like:
    //  Status status2 = StatusOk;
    //     Status status;
    //    status = status2;
    Status &operator=(const Status &other)
    {
        copyStatus(this, &other);
        return *this;
    }

    // Move assignment operator
    // http://thbecker.net/articles/rvalue_references/section_04.html
    // Handles cases like:
    //     Status status;
    //  status = someFunc();
    Status &operator=(Status &&other)
    {
        copyStatus(this, &other);
        return *this;
    }

    //
    // Operators
    //

    // Comparison operator
    bool operator==(const Status &other) const
    {
        return code() == other.code();
    }

    bool operator!=(const Status &other) const
    {
        // Invoke negation of the comparison operator
        return !(*this == other);
    }

    //
    // Status builders
    //

    explicit constexpr Status(StatusCode sts) : status_(sts) {}

    void fromStatusCode(StatusCode sts)
    {
        assert(isValidStatusCode(sts) && "we must have valid status codes");
        status_ = sts;
    }

    //
    // Accessors
    //
    inline bool ok() const { return status_ == StatusCodeOk; }

    inline StatusCode code() const { return status_; }

    inline const char *message() const { return strGetFromStatusCode(status_); }

  private:
    StatusCode status_;
};

inline const char *
strGetFromStatus(Status status)
{
    return status.message();
}

constexpr Status StatusOk(StatusCodeOk);
constexpr Status StatusPerm(StatusCodePerm);
constexpr Status StatusNoEnt(StatusCodeNoEnt);
constexpr Status StatusSrch(StatusCodeSrch);
constexpr Status StatusIntr(StatusCodeIntr);
constexpr Status StatusIO(StatusCodeIO);
constexpr Status StatusNxIO(StatusCodeNxIO);
constexpr Status Status2Big(StatusCode2Big);
constexpr Status StatusNoExec(StatusCodeNoExec);
constexpr Status StatusBadF(StatusCodeBadF);
constexpr Status StatusChild(StatusCodeChild);
constexpr Status StatusAgain(StatusCodeAgain);
constexpr Status StatusNoMem(StatusCodeNoMem);
constexpr Status StatusAccess(StatusCodeAccess);
constexpr Status StatusFault(StatusCodeFault);
constexpr Status StatusNotBlk(StatusCodeNotBlk);
constexpr Status StatusBusy(StatusCodeBusy);
constexpr Status StatusExist(StatusCodeExist);
constexpr Status StatusEof(StatusCodeEof);
constexpr Status StatusXDev(StatusCodeXDev);
constexpr Status StatusNoDev(StatusCodeNoDev);
constexpr Status StatusNotDir(StatusCodeNotDir);
constexpr Status StatusIsDir(StatusCodeIsDir);
constexpr Status StatusInval(StatusCodeInval);
constexpr Status StatusNFile(StatusCodeNFile);
constexpr Status StatusMFile(StatusCodeMFile);
constexpr Status StatusNoTTY(StatusCodeNoTTY);
constexpr Status StatusTxtBsy(StatusCodeTxtBsy);
constexpr Status StatusFBig(StatusCodeFBig);
constexpr Status StatusNoSpc(StatusCodeNoSpc);
constexpr Status StatusSPipe(StatusCodeSPipe);
constexpr Status StatusROFS(StatusCodeROFS);
constexpr Status StatusMLink(StatusCodeMLink);
constexpr Status StatusPipe(StatusCodePipe);
constexpr Status StatusDom(StatusCodeDom);
constexpr Status StatusRange(StatusCodeRange);
constexpr Status StatusDeadLk(StatusCodeDeadLk);
constexpr Status StatusNameTooLong(StatusCodeNameTooLong);
constexpr Status StatusNoLck(StatusCodeNoLck);
constexpr Status StatusNoSys(StatusCodeNoSys);
constexpr Status StatusNotEmpty(StatusCodeNotEmpty);
constexpr Status StatusLoop(StatusCodeLoop);
constexpr Status StatusNoMsg(StatusCodeNoMsg);
constexpr Status StatusIdRm(StatusCodeIdRm);
constexpr Status StatusChRng(StatusCodeChRng);
constexpr Status StatusL2NSync(StatusCodeL2NSync);
constexpr Status StatusL3Hlt(StatusCodeL3Hlt);
constexpr Status StatusL3Rst(StatusCodeL3Rst);
constexpr Status StatusLNRng(StatusCodeLNRng);
constexpr Status StatusUnatch(StatusCodeUnatch);
constexpr Status StatusNoCSI(StatusCodeNoCSI);
constexpr Status StatusL2Hlt(StatusCodeL2Hlt);
constexpr Status StatusBadE(StatusCodeBadE);
constexpr Status StatusBadR(StatusCodeBadR);
constexpr Status StatusXFull(StatusCodeXFull);
constexpr Status StatusNoAno(StatusCodeNoAno);
constexpr Status StatusBadRqC(StatusCodeBadRqC);
constexpr Status StatusBadSlt(StatusCodeBadSlt);
constexpr Status StatusBFont(StatusCodeBFont);
constexpr Status StatusNoStr(StatusCodeNoStr);
constexpr Status StatusNoData(StatusCodeNoData);
constexpr Status StatusTime(StatusCodeTime);
constexpr Status StatusNoSR(StatusCodeNoSR);
constexpr Status StatusNoNet(StatusCodeNoNet);
constexpr Status StatusNoPkg(StatusCodeNoPkg);
constexpr Status StatusRemote(StatusCodeRemote);
constexpr Status StatusNoLink(StatusCodeNoLink);
constexpr Status StatusAdv(StatusCodeAdv);
constexpr Status StatusSRMnt(StatusCodeSRMnt);
constexpr Status StatusComm(StatusCodeComm);
constexpr Status StatusProto(StatusCodeProto);
constexpr Status StatusMultihop(StatusCodeMultihop);
constexpr Status StatusDotDot(StatusCodeDotDot);
constexpr Status StatusBadMsg(StatusCodeBadMsg);
constexpr Status StatusOverflow(StatusCodeOverflow);
constexpr Status StatusNotUniq(StatusCodeNotUniq);
constexpr Status StatusBadFD(StatusCodeBadFD);
constexpr Status StatusRemChg(StatusCodeRemChg);
constexpr Status StatusLibAcc(StatusCodeLibAcc);
constexpr Status StatusLibBad(StatusCodeLibBad);
constexpr Status StatusLibScn(StatusCodeLibScn);
constexpr Status StatusLibMax(StatusCodeLibMax);
constexpr Status StatusLibExec(StatusCodeLibExec);
constexpr Status StatusIlSeq(StatusCodeIlSeq);
constexpr Status StatusRestart(StatusCodeRestart);
constexpr Status StatusStrPipe(StatusCodeStrPipe);
constexpr Status StatusUsers(StatusCodeUsers);
constexpr Status StatusNotSock(StatusCodeNotSock);
constexpr Status StatusDestAddrReq(StatusCodeDestAddrReq);
constexpr Status StatusMsgSize(StatusCodeMsgSize);
constexpr Status StatusPrototype(StatusCodePrototype);
constexpr Status StatusNoProtoOpt(StatusCodeNoProtoOpt);
constexpr Status StatusProtoNoSupport(StatusCodeProtoNoSupport);
constexpr Status StatusSockTNoSupport(StatusCodeSockTNoSupport);
constexpr Status StatusOpNotSupp(StatusCodeOpNotSupp);
constexpr Status StatusPFNoSupport(StatusCodePFNoSupport);
constexpr Status StatusAFNoSupport(StatusCodeAFNoSupport);
constexpr Status StatusAddrInUse(StatusCodeAddrInUse);
constexpr Status StatusAddrNotAvail(StatusCodeAddrNotAvail);
constexpr Status StatusNetDown(StatusCodeNetDown);
constexpr Status StatusNetUnreach(StatusCodeNetUnreach);
constexpr Status StatusNetReset(StatusCodeNetReset);
constexpr Status StatusConnAborted(StatusCodeConnAborted);
constexpr Status StatusConnReset(StatusCodeConnReset);
constexpr Status StatusNoBufs(StatusCodeNoBufs);
constexpr Status StatusIsConn(StatusCodeIsConn);
constexpr Status StatusNotConn(StatusCodeNotConn);
constexpr Status StatusShutdown(StatusCodeShutdown);
constexpr Status StatusTooManyRefs(StatusCodeTooManyRefs);
constexpr Status StatusTimedOut(StatusCodeTimedOut);
constexpr Status StatusConnRefused(StatusCodeConnRefused);
constexpr Status StatusHostDown(StatusCodeHostDown);
constexpr Status StatusHostUnreach(StatusCodeHostUnreach);
constexpr Status StatusAlready(StatusCodeAlready);
constexpr Status StatusInProgress(StatusCodeInProgress);
constexpr Status StatusStale(StatusCodeStale);
constexpr Status StatusUClean(StatusCodeUClean);
constexpr Status StatusNotNam(StatusCodeNotNam);
constexpr Status StatusNAvail(StatusCodeNAvail);
constexpr Status StatusIsNam(StatusCodeIsNam);
constexpr Status StatusRemoteIo(StatusCodeRemoteIo);
constexpr Status StatusDQuot(StatusCodeDQuot);
constexpr Status StatusNoMedium(StatusCodeNoMedium);
constexpr Status StatusMediumType(StatusCodeMediumType);
constexpr Status StatusCanceled(StatusCodeCanceled);
constexpr Status StatusNoKey(StatusCodeNoKey);
constexpr Status StatusKeyExpired(StatusCodeKeyExpired);
constexpr Status StatusKeyRevoked(StatusCodeKeyRevoked);
constexpr Status StatusKeyRejected(StatusCodeKeyRejected);
constexpr Status StatusOwnerDead(StatusCodeOwnerDead);
constexpr Status StatusNotRecoverable(StatusCodeNotRecoverable);
constexpr Status StatusRFKill(StatusCodeRFKill);
constexpr Status StatusHwPoison(StatusCodeHwPoison);
constexpr Status StatusTrunc(StatusCodeTrunc);
constexpr Status StatusUnimpl(StatusCodeUnimpl);
constexpr Status StatusUnknown(StatusCodeUnknown);
constexpr Status StatusMsgLibDeleteFailed(StatusCodeMsgLibDeleteFailed);
constexpr Status StatusThrCreateFailed(StatusCodeThrCreateFailed);
constexpr Status StatusThrAborted(StatusCodeThrAborted);
constexpr Status StatusConfigLibDevOpenFailed(StatusCodeConfigLibDevOpenFailed);
constexpr Status StatusConfigLibDevLSeekFailed(
    StatusCodeConfigLibDevLSeekFailed);
constexpr Status StatusConfigLibFlashDevOpenFailed(
    StatusCodeConfigLibFlashDevOpenFailed);
constexpr Status StatusConfigLibFlashDevLSeekFailed(
    StatusCodeConfigLibFlashDevLSeekFailed);
constexpr Status StatusConfigLibDeleteFailed(StatusCodeConfigLibDeleteFailed);
constexpr Status StatusUsrNodeIncorrectParams(StatusCodeUsrNodeIncorrectParams);
constexpr Status StatusUnicodeUnsupported(StatusCodeUnicodeUnsupported);
constexpr Status StatusEAIBadFlags(StatusCodeEAIBadFlags);
constexpr Status StatusEAINoName(StatusCodeEAINoName);
constexpr Status StatusEAIFail(StatusCodeEAIFail);
constexpr Status StatusEAIService(StatusCodeEAIService);
constexpr Status StatusEAINoData(StatusCodeEAINoData);
constexpr Status StatusEAIAddrFamily(StatusCodeEAIAddrFamily);
constexpr Status StatusEAINotCancel(StatusCodeEAINotCancel);
constexpr Status StatusEAIAllDone(StatusCodeEAIAllDone);
constexpr Status StatusEAIIDNEncode(StatusCodeEAIIDNEncode);
constexpr Status StatusLast(StatusCodeLast);
constexpr Status StatusMore(StatusCodeMore);
constexpr Status StatusCliUnknownCmd(StatusCodeCliUnknownCmd);
constexpr Status StatusCliParseError(StatusCodeCliParseError);
constexpr Status StatusSchedQueueLenExceeded(StatusCodeSchedQueueLenExceeded);
constexpr Status StatusMsgFail(StatusCodeMsgFail);
constexpr Status StatusMsgOutOfMessages(StatusCodeMsgOutOfMessages);
constexpr Status StatusMsgShutdown(StatusCodeMsgShutdown);
constexpr Status StatusNoSuchNode(StatusCodeNoSuchNode);
constexpr Status StatusNewTableCreated(StatusCodeNewTableCreated);
constexpr Status StatusNoSuchResultSet(StatusCodeNoSuchResultSet);
constexpr Status StatusDfAppendUnsupported(StatusCodeDfAppendUnsupported);
constexpr Status StatusDfRemoveUnsupported(StatusCodeDfRemoveUnsupported);
constexpr Status StatusDfParseError(StatusCodeDfParseError);
constexpr Status StatusDfRecordCorrupt(StatusCodeDfRecordCorrupt);
constexpr Status StatusDfFieldNoExist(StatusCodeDfFieldNoExist);
constexpr Status StatusDfUnknownFieldType(StatusCodeDfUnknownFieldType);
constexpr Status StatusDfRecordNotFound(StatusCodeDfRecordNotFound);
constexpr Status StatusDfValNotFound(StatusCodeDfValNotFound);
constexpr Status StatusDfInvalidFormat(StatusCodeDfInvalidFormat);
constexpr Status StatusDfLocalFatptrOnly(StatusCodeDfLocalFatptrOnly);
constexpr Status StatusDfValuesBufTooSmall(StatusCodeDfValuesBufTooSmall);
constexpr Status StatusDfMaxValuesPerFieldExceeded(
    StatusCodeDfMaxValuesPerFieldExceeded);
constexpr Status StatusDfFieldTypeUnsupported(StatusCodeDfFieldTypeUnsupported);
constexpr Status StatusDfMaxDictionarySegmentsExceeded(
    StatusCodeDfMaxDictionarySegmentsExceeded);
constexpr Status StatusDfBadRecordId(StatusCodeDfBadRecordId);
constexpr Status StatusDfMaxRecordsExceeded(StatusCodeDfMaxRecordsExceeded);
constexpr Status StatusDfTypeMismatch(StatusCodeDfTypeMismatch);
constexpr Status StatusDsTooManyKeyValues(StatusCodeDsTooManyKeyValues);
constexpr Status StatusDsNotFound(StatusCodeDsNotFound);
constexpr Status StatusDsLoadAlreadyStarted(StatusCodeDsLoadAlreadyStarted);
constexpr Status StatusDsUrlTooLong(StatusCodeDsUrlTooLong);
constexpr Status StatusDsInvalidUrl(StatusCodeDsInvalidUrl);
constexpr Status StatusDsCreateNotSupported(StatusCodeDsCreateNotSupported);
constexpr Status StatusDsUnlinkNotSupported(StatusCodeDsUnlinkNotSupported);
constexpr Status StatusDsRenameNotSupported(StatusCodeDsRenameNotSupported);
constexpr Status StatusDsWriteNotSupported(StatusCodeDsWriteNotSupported);
constexpr Status StatusDsSeekNotSupported(StatusCodeDsSeekNotSupported);
constexpr Status StatusDsSeekFailed(StatusCodeDsSeekFailed);
constexpr Status StatusDsMkDirNotSupported(StatusCodeDsMkDirNotSupported);
constexpr Status StatusDsRmDirNotSupported(StatusCodeDsRmDirNotSupported);
constexpr Status StatusDsLoadFailed(StatusCodeDsLoadFailed);
constexpr Status StatusDsDatasetInUse(StatusCodeDsDatasetInUse);
constexpr Status StatusDsFormatTypeUnsupported(
    StatusCodeDsFormatTypeUnsupported);
constexpr Status StatusDsMysqlInitFailed(StatusCodeDsMysqlInitFailed);
constexpr Status StatusDsMysqlConnectFailed(StatusCodeDsMysqlConnectFailed);
constexpr Status StatusDsMysqlQueryFailed(StatusCodeDsMysqlQueryFailed);
constexpr Status StatusExODBCConnectFailed(StatusCodeExODBCConnectFailed);
constexpr Status StatusExODBCCleanupFailed(StatusCodeExODBCCleanupFailed);
constexpr Status StatusExODBCAddNotSupported(StatusCodeExODBCAddNotSupported);
constexpr Status StatusExODBCBindFailed(StatusCodeExODBCBindFailed);
constexpr Status StatusExODBCTableCreationFailed(
    StatusCodeExODBCTableCreationFailed);
constexpr Status StatusExODBCExportFailed(StatusCodeExODBCExportFailed);
constexpr Status StatusExODBCTableExists(StatusCodeExODBCTableExists);
constexpr Status StatusExODBCTableDoesntExist(StatusCodeExODBCTableDoesntExist);
constexpr Status StatusExTargetListRace(StatusCodeExTargetListRace);
constexpr Status StatusExTargetAlreadyExists(StatusCodeExTargetAlreadyExists);
constexpr Status StatusDsGetFileAttrNotSupported(
    StatusCodeDsGetFileAttrNotSupported);
constexpr Status StatusDsGetFileAttrCompressed(
    StatusCodeDsGetFileAttrCompressed);
constexpr Status StatusReallocShrinkFailed(StatusCodeReallocShrinkFailed);
constexpr Status StatusNsObjAlreadyExists(StatusCodeNsObjAlreadyExists);
constexpr Status StatusTableAlreadyExists(StatusCodeTableAlreadyExists);
constexpr Status StatusCliUnclosedQuotes(StatusCodeCliUnclosedQuotes);
constexpr Status StatusRangePartError(StatusCodeRangePartError);
constexpr Status StatusNewFieldNameIsBlank(StatusCodeNewFieldNameIsBlank);
constexpr Status StatusNoDataDictForFormatType(
    StatusCodeNoDataDictForFormatType);
constexpr Status StatusBTreeNotFound(StatusCodeBTreeNotFound);
constexpr Status StatusBTreeKeyTypeMismatch(StatusCodeBTreeKeyTypeMismatch);
constexpr Status StatusBTreeDatasetMismatch(StatusCodeBTreeDatasetMismatch);
constexpr Status StatusCmdNotComplete(StatusCodeCmdNotComplete);
constexpr Status StatusInvalidResultSetId(StatusCodeInvalidResultSetId);
constexpr Status StatusPositionExceedResultSetSize(
    StatusCodePositionExceedResultSetSize);
constexpr Status StatusHandleInUse(StatusCodeHandleInUse);
constexpr Status StatusCliLineTooLong(StatusCodeCliLineTooLong);
constexpr Status StatusCliErrorReadFromFile(StatusCodeCliErrorReadFromFile);
constexpr Status StatusInvalidTableName(StatusCodeInvalidTableName);
constexpr Status StatusNsObjNameTooLong(StatusCodeNsObjNameTooLong);
constexpr Status StatusApiUnexpectedEOF(StatusCodeApiUnexpectedEOF);
constexpr Status StatusStatsInvalidGroupId(StatusCodeStatsInvalidGroupId);
constexpr Status StatusStatsInvalidGroupName(StatusCodeStatsInvalidGroupName);
constexpr Status StatusInvalidHandle(StatusCodeInvalidHandle);
constexpr Status StatusThriftProtocolError(StatusCodeThriftProtocolError);
constexpr Status StatusBTreeHasNoRoot(StatusCodeBTreeHasNoRoot);
constexpr Status StatusBTreeKeyNotFound(StatusCodeBTreeKeyNotFound);
constexpr Status StatusQaKeyValuePairNotFound(StatusCodeQaKeyValuePairNotFound);
constexpr Status StatusAstMalformedEvalString(StatusCodeAstMalformedEvalString);
constexpr Status StatusAstNoSuchFunction(StatusCodeAstNoSuchFunction);
constexpr Status StatusAstWrongNumberOfArgs(StatusCodeAstWrongNumberOfArgs);
constexpr Status StatusFieldNameTooLong(StatusCodeFieldNameTooLong);
constexpr Status StatusFieldNameAlreadyExists(StatusCodeFieldNameAlreadyExists);
constexpr Status StatusXdfWrongNumberOfArgs(StatusCodeXdfWrongNumberOfArgs);
constexpr Status StatusXdfUnaryOperandExpected(
    StatusCodeXdfUnaryOperandExpected);
constexpr Status StatusXdfTypeUnsupported(StatusCodeXdfTypeUnsupported);
constexpr Status StatusXdfDivByZero(StatusCodeXdfDivByZero);
constexpr Status StatusXdfFloatNan(StatusCodeXdfFloatNan);
constexpr Status StatusXdfMixedTypeNotSupported(
    StatusCodeXdfMixedTypeNotSupported);
constexpr Status StatusXdfAggregateOverflow(StatusCodeXdfAggregateOverflow);
constexpr Status StatusKvNotFound(StatusCodeKvNotFound);
constexpr Status StatusXdbSlotPrettyVacant(StatusCodeXdbSlotPrettyVacant);
constexpr Status StatusNoDataInXdb(StatusCodeNoDataInXdb);
constexpr Status StatusXdbLoadInProgress(StatusCodeXdbLoadInProgress);
constexpr Status StatusXdbNotFound(StatusCodeXdbNotFound);
constexpr Status StatusXdbUninitializedCursor(StatusCodeXdbUninitializedCursor);
constexpr Status StatusQrTaskFailed(StatusCodeQrTaskFailed);
constexpr Status StatusQrIdNonExist(StatusCodeQrIdNonExist);
constexpr Status StatusQrJobNonExist(StatusCodeQrJobNonExist);
constexpr Status StatusQrJobRunning(StatusCodeQrJobRunning);
constexpr Status StatusApiTaskFailed(StatusCodeApiTaskFailed);
constexpr Status StatusAlreadyIndexed(StatusCodeAlreadyIndexed);
constexpr Status StatusEvalUnsubstitutedVariables(
    StatusCodeEvalUnsubstitutedVariables);
constexpr Status StatusKvDstFull(StatusCodeKvDstFull);
constexpr Status StatusModuleNotInit(StatusCodeModuleNotInit);
constexpr Status StatusMaxJoinFieldsExceeded(StatusCodeMaxJoinFieldsExceeded);
constexpr Status StatusXdbKeyTypeAlreadySet(StatusCodeXdbKeyTypeAlreadySet);
constexpr Status StatusJoinTypeMismatch(StatusCodeJoinTypeMismatch);
constexpr Status StatusJoinDhtMismatch(StatusCodeJoinDhtMismatch);
constexpr Status StatusFailed(StatusCodeFailed);
constexpr Status StatusIllegalFileName(StatusCodeIllegalFileName);
constexpr Status StatusEmptyFile(StatusCodeEmptyFile);
constexpr Status StatusEvalStringTooLong(StatusCodeEvalStringTooLong);
constexpr Status StatusTableDeleted(StatusCodeTableDeleted);
constexpr Status StatusFailOpenFile(StatusCodeFailOpenFile);
constexpr Status StatusQueryFailed(StatusCodeQueryFailed);
constexpr Status StatusQueryNeedsNewSession(StatusCodeQueryNeedsNewSession);
constexpr Status StatusCreateDagNodeFailed(StatusCodeCreateDagNodeFailed);
constexpr Status StatusDeleteDagNodeFailed(StatusCodeDeleteDagNodeFailed);
constexpr Status StatusRenameDagNodeFailed(StatusCodeRenameDagNodeFailed);
constexpr Status StatusChangeDagNodeStateFailed(
    StatusCodeChangeDagNodeStateFailed);
constexpr Status StatusAggregateNoSuchField(StatusCodeAggregateNoSuchField);
constexpr Status StatusAggregateLocalFnNeedArgument(
    StatusCodeAggregateLocalFnNeedArgument);
constexpr Status StatusAggregateAccNotInited(StatusCodeAggregateAccNotInited);
constexpr Status StatusAggregateReturnValueNotScalar(
    StatusCodeAggregateReturnValueNotScalar);
constexpr Status StatusNsMaximumObjectsReached(
    StatusCodeNsMaximumObjectsReached);
constexpr Status StatusNsObjInUse(StatusCodeNsObjInUse);
constexpr Status StatusNsInvalidObjName(StatusCodeNsInvalidObjName);
constexpr Status StatusNsNotFound(StatusCodeNsNotFound);
constexpr Status StatusDagNodeNotFound(StatusCodeDagNodeNotFound);
constexpr Status StatusUpdateDagNodeOperationNotSupported(
    StatusCodeUpdateDagNodeOperationNotSupported);
constexpr Status StatusMsgMaxPayloadExceeded(StatusCodeMsgMaxPayloadExceeded);
constexpr Status StatusKvEntryNotFound(StatusCodeKvEntryNotFound);
constexpr Status StatusKvEntryNotEqual(StatusCodeKvEntryNotEqual);
constexpr Status StatusStatsCouldNotGetMemUsedInfo(
    StatusCodeStatsCouldNotGetMemUsedInfo);
constexpr Status StatusStatusFieldNotInited(StatusCodeStatusFieldNotInited);
constexpr Status StatusAggNoSuchFunction(StatusCodeAggNoSuchFunction);
constexpr Status StatusWaitKeyTimeout(StatusCodeWaitKeyTimeout);
constexpr Status StatusVariableNameTooLong(StatusCodeVariableNameTooLong);
constexpr Status StatusDgDagNotFound(StatusCodeDgDagNotFound);
constexpr Status StatusDgInvalidDagName(StatusCodeDgInvalidDagName);
constexpr Status StatusDgDagNameTooLong(StatusCodeDgDagNameTooLong);
constexpr Status StatusDgDagAlreadyExists(StatusCodeDgDagAlreadyExists);
constexpr Status StatusDgDagEmpty(StatusCodeDgDagEmpty);
constexpr Status StatusDgDagNotEmpty(StatusCodeDgDagNotEmpty);
constexpr Status StatusDgDagNoMore(StatusCodeDgDagNoMore);
constexpr Status StatusDgDagReserved(StatusCodeDgDagReserved);
constexpr Status StatusDgNodeInUse(StatusCodeDgNodeInUse);
constexpr Status StatusDgDagNodeError(StatusCodeDgDagNodeError);
constexpr Status StatusDgOperationNotSupported(
    StatusCodeDgOperationNotSupported);
constexpr Status StatusDgDagNodeNotReady(StatusCodeDgDagNodeNotReady);
constexpr Status StatusDgFailToDestroyHandle(StatusCodeDgFailToDestroyHandle);
constexpr Status StatusDsDatasetLoaded(StatusCodeDsDatasetLoaded);
constexpr Status StatusDsDatasetNotReady(StatusCodeDsDatasetNotReady);
constexpr Status StatusSessionNotFound(StatusCodeSessionNotFound);
constexpr Status StatusSessionExists(StatusCodeSessionExists);
constexpr Status StatusSessionNotInact(StatusCodeSessionNotInact);
constexpr Status StatusSessionUsrNameInvalid(StatusCodeSessionUsrNameInvalid);
constexpr Status StatusSessionError(StatusCodeSessionError);
constexpr Status StatusSessionUsrAlreadyExists(
    StatusCodeSessionUsrAlreadyExists);
constexpr Status StatusDgDeleteOperationNotPermitted(
    StatusCodeDgDeleteOperationNotPermitted);
constexpr Status StatusUdfModuleLoadFailed(StatusCodeUdfModuleLoadFailed);
constexpr Status StatusUdfModuleAlreadyExists(StatusCodeUdfModuleAlreadyExists);
constexpr Status StatusUdfModuleNotFound(StatusCodeUdfModuleNotFound);
constexpr Status StatusUdfModuleEmpty(StatusCodeUdfModuleEmpty);
constexpr Status StatusUdfModuleInvalidName(StatusCodeUdfModuleInvalidName);
constexpr Status StatusUdfModuleInvalidType(StatusCodeUdfModuleInvalidType);
constexpr Status StatusUdfModuleInvalidSource(StatusCodeUdfModuleInvalidSource);
constexpr Status StatusUdfModuleSourceTooLarge(
    StatusCodeUdfModuleSourceTooLarge);
constexpr Status StatusUdfFunctionLoadFailed(StatusCodeUdfFunctionLoadFailed);
constexpr Status StatusUdfFunctionNotFound(StatusCodeUdfFunctionNotFound);
constexpr Status StatusUdfFunctionNameTooLong(StatusCodeUdfFunctionNameTooLong);
constexpr Status StatusUdfFunctionTooManyParams(
    StatusCodeUdfFunctionTooManyParams);
constexpr Status StatusUdfVarNameTooLong(StatusCodeUdfVarNameTooLong);
constexpr Status StatusUdfUnsupportedType(StatusCodeUdfUnsupportedType);
constexpr Status StatusUdfPersistInvalid(StatusCodeUdfPersistInvalid);
constexpr Status StatusUdfPyConvert(StatusCodeUdfPyConvert);
constexpr Status StatusUdfExecuteFailed(StatusCodeUdfExecuteFailed);
constexpr Status StatusUdfInval(StatusCodeUdfInval);
constexpr Status StatusUdfDeletePartial(StatusCodeUdfDeletePartial);
constexpr Status StatusXcalarEvalTokenNameTooLong(
    StatusCodeXcalarEvalTokenNameTooLong);
constexpr Status StatusNoConfigFile(StatusCodeNoConfigFile);
constexpr Status StatusCouldNotResolveSchema(StatusCodeCouldNotResolveSchema);
constexpr Status StatusDhtEmptyDhtName(StatusCodeDhtEmptyDhtName);
constexpr Status StatusDhtUpperBoundLessThanLowerBound(
    StatusCodeDhtUpperBoundLessThanLowerBound);
constexpr Status StatusLogChecksumFailed(StatusCodeLogChecksumFailed);
constexpr Status StatusDhtDoesNotPreserveOrder(
    StatusCodeDhtDoesNotPreserveOrder);
constexpr Status StatusLogMaximumEntrySizeExceeded(
    StatusCodeLogMaximumEntrySizeExceeded);
constexpr Status StatusLogCorruptHeader(StatusCodeLogCorruptHeader);
constexpr Status StatusLogCorrupt(StatusCodeLogCorrupt);
constexpr Status StatusLogVersionMismatch(StatusCodeLogVersionMismatch);
constexpr Status StatusKvInvalidKeyChar(StatusCodeKvInvalidKeyChar);
constexpr Status StatusDhtProtected(StatusCodeDhtProtected);
constexpr Status StatusKvStoreNotFound(StatusCodeKvStoreNotFound);
constexpr Status StatusSSE42Unsupported(StatusCodeSSE42Unsupported);
constexpr Status StatusPyBadUdfName(StatusCodePyBadUdfName);
constexpr Status StatusLicInputInvalid(StatusCodeLicInputInvalid);
constexpr Status StatusLicFileOpen(StatusCodeLicFileOpen);
constexpr Status StatusLicFileRead(StatusCodeLicFileRead);
constexpr Status StatusLicFileWrite(StatusCodeLicFileWrite);
constexpr Status StatusLicPubKeyMissing(StatusCodeLicPubKeyMissing);
constexpr Status StatusLicPubKeyErr(StatusCodeLicPubKeyErr);
constexpr Status StatusLicPubKeyIdx(StatusCodeLicPubKeyIdx);
constexpr Status StatusLicMissing(StatusCodeLicMissing);
constexpr Status StatusLicErr(StatusCodeLicErr);
constexpr Status StatusLicSignatureInvalid(StatusCodeLicSignatureInvalid);
constexpr Status StatusLicBase32MapInvalid(StatusCodeLicBase32MapInvalid);
constexpr Status StatusLicBase32ValInvalid(StatusCodeLicBase32ValInvalid);
constexpr Status StatusLicMD5Invalid(StatusCodeLicMD5Invalid);
constexpr Status StatusLicUnkError(StatusCodeLicUnkError);
constexpr Status StatusLicInvalid(StatusCodeLicInvalid);
constexpr Status StatusLicWrongSize(StatusCodeLicWrongSize);
constexpr Status StatusLicExpired(StatusCodeLicExpired);
constexpr Status StatusLicOldVersion(StatusCodeLicOldVersion);
constexpr Status StatusLicInsufficientNodes(StatusCodeLicInsufficientNodes);
constexpr Status StatusLogHandleClosed(StatusCodeLogHandleClosed);
constexpr Status StatusLogHandleInvalid(StatusCodeLogHandleInvalid);
constexpr Status StatusShutdownInProgress(StatusCodeShutdownInProgress);
constexpr Status StatusOrderingNotSupported(StatusCodeOrderingNotSupported);
constexpr Status StatusHdfsNoConnect(StatusCodeHdfsNoConnect);
constexpr Status StatusHdfsNoDirectoryListing(StatusCodeHdfsNoDirectoryListing);
constexpr Status StatusCliCanvasTooSmall(StatusCodeCliCanvasTooSmall);
constexpr Status StatusDagParamInputTypeMismatch(
    StatusCodeDagParamInputTypeMismatch);
constexpr Status StatusParameterTooLong(StatusCodeParameterTooLong);
constexpr Status StatusExceedMaxScheduleTime(StatusCodeExceedMaxScheduleTime);
constexpr Status StatusExceedMaxSchedulePeriod(
    StatusCodeExceedMaxSchedulePeriod);
constexpr Status StatusXcalarApiNotParameterizable(
    StatusCodeXcalarApiNotParameterizable);
constexpr Status StatusQrNotFound(StatusCodeQrNotFound);
constexpr Status StatusJoinOrderingMismatch(StatusCodeJoinOrderingMismatch);
constexpr Status StatusInvalidUserCookie(StatusCodeInvalidUserCookie);
constexpr Status StatusStTooManySchedTask(StatusCodeStTooManySchedTask);
constexpr Status StatusRowUnfinished(StatusCodeRowUnfinished);
constexpr Status StatusInputTooLarge(StatusCodeInputTooLarge);
constexpr Status StatusConfigInvalid(StatusCodeConfigInvalid);
constexpr Status StatusInvalNodeId(StatusCodeInvalNodeId);
constexpr Status StatusNoLocalNodes(StatusCodeNoLocalNodes);
constexpr Status StatusDsFallocateNotSupported(
    StatusCodeDsFallocateNotSupported);
constexpr Status StatusNoExtension(StatusCodeNoExtension);
constexpr Status StatusExportTargetNotSupported(
    StatusCodeExportTargetNotSupported);
constexpr Status StatusExportInvalidCreateRule(
    StatusCodeExportInvalidCreateRule);
constexpr Status StatusExportNoColumns(StatusCodeExportNoColumns);
constexpr Status StatusExportTooManyColumns(StatusCodeExportTooManyColumns);
constexpr Status StatusExportColumnNameTooLong(
    StatusCodeExportColumnNameTooLong);
constexpr Status StatusExportEmptyResultSet(StatusCodeExportEmptyResultSet);
constexpr Status StatusExportUnresolvedSchema(StatusCodeExportUnresolvedSchema);
constexpr Status StatusExportSFFileExists(StatusCodeExportSFFileExists);
constexpr Status StatusExportSFFileDoesntExist(
    StatusCodeExportSFFileDoesntExist);
constexpr Status StatusMonPortInvalid(StatusCodeMonPortInvalid);
constexpr Status StatusExportSFFileDirDuplicate(
    StatusCodeExportSFFileDirDuplicate);
constexpr Status StatusExportSFFileCorrupted(StatusCodeExportSFFileCorrupted);
constexpr Status StatusExportSFFileRuleNeedsNewFile(
    StatusCodeExportSFFileRuleNeedsNewFile);
constexpr Status StatusExportSFFileRuleSizeTooSmall(
    StatusCodeExportSFFileRuleSizeTooSmall);
constexpr Status StatusExportSFSingleSplitConflict(
    StatusCodeExportSFSingleSplitConflict);
constexpr Status StatusExportSFAppendSepConflict(
    StatusCodeExportSFAppendSepConflict);
constexpr Status StatusExportSFAppendSingleHeader(
    StatusCodeExportSFAppendSingleHeader);
constexpr Status StatusExportSFInvalidHeaderType(
    StatusCodeExportSFInvalidHeaderType);
constexpr Status StatusExportSFInvalidSplitType(
    StatusCodeExportSFInvalidSplitType);
constexpr Status StatusExportSFMaxSizeZero(StatusCodeExportSFMaxSizeZero);
constexpr Status StatusVersionMismatch(StatusCodeVersionMismatch);
constexpr Status StatusFileCorrupt(StatusCodeFileCorrupt);
constexpr Status StatusApiFunctionInvalid(StatusCodeApiFunctionInvalid);
constexpr Status StatusLibArchiveError(StatusCodeLibArchiveError);
constexpr Status StatusSendSocketFail(StatusCodeSendSocketFail);
constexpr Status StatusNodeSkipped(StatusCodeNodeSkipped);
constexpr Status StatusDfCastTruncationOccurred(
    StatusCodeDfCastTruncationOccurred);
constexpr Status StatusEvalCastError(StatusCodeEvalCastError);
constexpr Status StatusLogUnaligned(StatusCodeLogUnaligned);
constexpr Status StatusStrEncodingNotSupported(
    StatusCodeStrEncodingNotSupported);
constexpr Status StatusShmsgInterfaceClosed(StatusCodeShmsgInterfaceClosed);
constexpr Status StatusOperationHasFinished(StatusCodeOperationHasFinished);
constexpr Status StatusOpstatisticsNotAvail(StatusCodeOpstatisticsNotAvail);
constexpr Status StatusRetinaParseError(StatusCodeRetinaParseError);
constexpr Status StatusRetinaTooManyColumns(StatusCodeRetinaTooManyColumns);
constexpr Status StatusUdfModuleOverwrittenSuccessfully(
    StatusCodeUdfModuleOverwrittenSuccessfully);
constexpr Status StatusSupportFail(StatusCodeSupportFail);
constexpr Status StatusShmsgPayloadTooLarge(StatusCodeShmsgPayloadTooLarge);
constexpr Status StatusNoChild(StatusCodeNoChild);
constexpr Status StatusChildTerminated(StatusCodeChildTerminated);
constexpr Status StatusXdbMaxSgElemsExceeded(StatusCodeXdbMaxSgElemsExceeded);
constexpr Status StatusAggregateResultNotFound(
    StatusCodeAggregateResultNotFound);
constexpr Status StatusMaxRowSizeExceeded(StatusCodeMaxRowSizeExceeded);
constexpr Status StatusMaxDirectoryDepthExceeded(
    StatusCodeMaxDirectoryDepthExceeded);
constexpr Status StatusDirectorySubdirOpenFailed(
    StatusCodeDirectorySubdirOpenFailed);
constexpr Status StatusInvalidDatasetName(StatusCodeInvalidDatasetName);
constexpr Status StatusMaxStatsGroupExceeded(StatusCodeMaxStatsGroupExceeded);
constexpr Status StatusLrqDuplicateUserDefinedFields(
    StatusCodeLrqDuplicateUserDefinedFields);
constexpr Status StatusTypeConversionError(StatusCodeTypeConversionError);
constexpr Status StatusNotSupportedInProdBuild(
    StatusCodeNotSupportedInProdBuild);
constexpr Status StatusOutOfFaultInjModuleSlots(
    StatusCodeOutOfFaultInjModuleSlots);
constexpr Status StatusNoSuchErrorpointModule(StatusCodeNoSuchErrorpointModule);
constexpr Status StatusNoSuchErrorpoint(StatusCodeNoSuchErrorpoint);
constexpr Status StatusAllFilesEmpty(StatusCodeAllFilesEmpty);
constexpr Status StatusStatsGroupNameTooLong(StatusCodeStatsGroupNameTooLong);
constexpr Status StatusStatsNameTooLong(StatusCodeStatsNameTooLong);
constexpr Status StatusMaxStatsExceeded(StatusCodeMaxStatsExceeded);
constexpr Status StatusStatsGroupIsFull(StatusCodeStatsGroupIsFull);
constexpr Status StatusNoMatchingFiles(StatusCodeNoMatchingFiles);
constexpr Status StatusFieldNotFound(StatusCodeFieldNotFound);
constexpr Status StatusImmediateNameCollision(StatusCodeImmediateNameCollision);
constexpr Status StatusFatptrPrefixCollision(StatusCodeFatptrPrefixCollision);
constexpr Status StatusListFilesNotSupported(StatusCodeListFilesNotSupported);
constexpr Status StatusAlreadyLoadDone(StatusCodeAlreadyLoadDone);
constexpr Status StatusSkipRecordNeedsDelim(StatusCodeSkipRecordNeedsDelim);
constexpr Status StatusNoParent(StatusCodeNoParent);
constexpr Status StatusRebuildDagFailed(StatusCodeRebuildDagFailed);
constexpr Status StatusStackSizeTooSmall(StatusCodeStackSizeTooSmall);
constexpr Status StatusTargetDoesntExist(StatusCodeTargetDoesntExist);
constexpr Status StatusExODBCRemoveNotSupported(
    StatusCodeExODBCRemoveNotSupported);
constexpr Status StatusFunctionalTestDisabled(StatusCodeFunctionalTestDisabled);
constexpr Status StatusFunctionalTestNumFuncTestExceeded(
    StatusCodeFunctionalTestNumFuncTestExceeded);
constexpr Status StatusTargetCorrupted(StatusCodeTargetCorrupted);
constexpr Status StatusUdfPyConvertFromFailed(StatusCodeUdfPyConvertFromFailed);
constexpr Status StatusHdfsWRNotSupported(StatusCodeHdfsWRNotSupported);
constexpr Status StatusFunctionalTestNoTablesLeft(
    StatusCodeFunctionalTestNoTablesLeft);
constexpr Status StatusFunctionalTestTableEmpty(
    StatusCodeFunctionalTestTableEmpty);
constexpr Status StatusRegexCompileFailed(StatusCodeRegexCompileFailed);
constexpr Status StatusUdfNotFound(StatusCodeUdfNotFound);
constexpr Status StatusApisWorkTooManyOutstanding(
    StatusCodeApisWorkTooManyOutstanding);
constexpr Status StatusInvalidUserNameLen(StatusCodeInvalidUserNameLen);
constexpr Status StatusUdfPyInjectFailed(StatusCodeUdfPyInjectFailed);
constexpr Status StatusUsrNodeInited(StatusCodeUsrNodeInited);
constexpr Status StatusFileListParseError(StatusCodeFileListParseError);
constexpr Status StatusLoadArgsInvalid(StatusCodeLoadArgsInvalid);
constexpr Status StatusAllWorkDone(StatusCodeAllWorkDone);
constexpr Status StatusUdfAlreadyExists(StatusCodeUdfAlreadyExists);
constexpr Status StatusUdfFunctionTooFewParams(
    StatusCodeUdfFunctionTooFewParams);
constexpr Status StatusDgOperationInError(StatusCodeDgOperationInError);
constexpr Status StatusAppNameInvalid(StatusCodeAppNameInvalid);
constexpr Status StatusAppHostTypeInvalid(StatusCodeAppHostTypeInvalid);
constexpr Status StatusAppExecTooBig(StatusCodeAppExecTooBig);
constexpr Status StatusRccInitErr(StatusCodeRccInitErr);
constexpr Status StatusRccDefault(StatusCodeRccDefault);
constexpr Status StatusRccNotFound(StatusCodeRccNotFound);
constexpr Status StatusRccElemNotFound(StatusCodeRccElemNotFound);
constexpr Status StatusRccIncompatibleState(StatusCodeRccIncompatibleState);
constexpr Status StatusGvmInvalidAction(StatusCodeGvmInvalidAction);
constexpr Status StatusGlobalVariableNotFound(StatusCodeGlobalVariableNotFound);
constexpr Status StatusCorruptedOutputSize(StatusCodeCorruptedOutputSize);
constexpr Status StatusDatasetNameAlreadyExists(
    StatusCodeDatasetNameAlreadyExists);
constexpr Status StatusDatasetAlreadyDeleted(StatusCodeDatasetAlreadyDeleted);
constexpr Status StatusRetinaNotFound(StatusCodeRetinaNotFound);
constexpr Status StatusDhtNotFound(StatusCodeDhtNotFound);
constexpr Status StatusTableNotFound(StatusCodeTableNotFound);
constexpr Status StatusRetinaTooManyParameters(
    StatusCodeRetinaTooManyParameters);
constexpr Status StatusConfigParamImmutable(StatusCodeConfigParamImmutable);
constexpr Status StatusOperationInError(StatusCodeOperationInError);
constexpr Status StatusOperationCancelled(StatusCodeOperationCancelled);
constexpr Status StatusQrQueryNotExist(StatusCodeQrQueryNotExist);
constexpr Status StatusDgParentNodeNotExist(StatusCodeDgParentNodeNotExist);
constexpr Status StatusLoadAppNotExist(StatusCodeLoadAppNotExist);
constexpr Status StatusAppOutParseFail(StatusCodeAppOutParseFail);
constexpr Status StatusFaultInjection(StatusCodeFaultInjection);
constexpr Status StatusFaultInjection2PC(StatusCodeFaultInjection2PC);
constexpr Status StatusExportAppNotExist(StatusCodeExportAppNotExist);
constexpr Status StatusSessionUsrInUse(StatusCodeSessionUsrInUse);
constexpr Status StatusNoXdbPageBcMem(StatusCodeNoXdbPageBcMem);
constexpr Status StatusAppFlagsInvalid(StatusCodeAppFlagsInvalid);
constexpr Status StatusQueryJobProcessing(StatusCodeQueryJobProcessing);
constexpr Status StatusTwoPcBarMsgInvalid(StatusCodeTwoPcBarMsgInvalid);
constexpr Status StatusTwoPcBarTimeout(StatusCodeTwoPcBarTimeout);
constexpr Status StatusTooManyChildren(StatusCodeTooManyChildren);
constexpr Status StatusMaxFileLimitReached(StatusCodeMaxFileLimitReached);
constexpr Status StatusApiWouldBlock(StatusCodeApiWouldBlock);
constexpr Status StatusExportSFSingleHeaderConflict(
    StatusCodeExportSFSingleHeaderConflict);
constexpr Status StatusAggFnInClass1Ast(StatusCodeAggFnInClass1Ast);
constexpr Status StatusDagNodeDropped(StatusCodeDagNodeDropped);
constexpr Status StatusXdbSlotHasActiveCursor(StatusCodeXdbSlotHasActiveCursor);
constexpr Status StatusProtobufDecodeError(StatusCodeProtobufDecodeError);
constexpr Status StatusAppLoadFailed(StatusCodeAppLoadFailed);
constexpr Status StatusAppDoesNotExist(StatusCodeAppDoesNotExist);
constexpr Status StatusNotShared(StatusCodeNotShared);
constexpr Status StatusProtobufEncodeError(StatusCodeProtobufEncodeError);
constexpr Status StatusJsonError(StatusCodeJsonError);
constexpr Status StatusMsgStreamNotFound(StatusCodeMsgStreamNotFound);
constexpr Status StatusUnderflow(StatusCodeUnderflow);
constexpr Status StatusPageCacheFull(StatusCodePageCacheFull);
constexpr Status StatusSchedTaskFunctionalityRemoved(
    StatusCodeSchedTaskFunctionalityRemoved);
constexpr Status StatusPendingRemoval(StatusCodePendingRemoval);
constexpr Status StatusAppFailedToGetOutput(StatusCodeAppFailedToGetOutput);
constexpr Status StatusAppFailedToGetError(StatusCodeAppFailedToGetError);
constexpr Status StatusNsInternalTableError(StatusCodeNsInternalTableError);
constexpr Status StatusNsStale(StatusCodeNsStale);
constexpr Status StatusDurHandleNoInit(StatusCodeDurHandleNoInit);
constexpr Status StatusDurVerError(StatusCodeDurVerError);
constexpr Status StatusDurDirtyWriter(StatusCodeDurDirtyWriter);
constexpr Status StatusMaxFieldSizeExceeded(StatusCodeMaxFieldSizeExceeded);
constexpr Status StatusQrQueryAlreadyExists(StatusCodeQrQueryAlreadyExists);
constexpr Status StatusUdfModuleInUse(StatusCodeUdfModuleInUse);
constexpr Status StatusTargetInUse(StatusCodeTargetInUse);
constexpr Status StatusOperationOutstanding(StatusCodeOperationOutstanding);
constexpr Status StatusDhtAlreadyExists(StatusCodeDhtAlreadyExists);
constexpr Status StatusDhtInUse(StatusCodeDhtInUse);
constexpr Status StatusTooManyResultSets(StatusCodeTooManyResultSets);
constexpr Status StatusRetinaAlreadyExists(StatusCodeRetinaAlreadyExists);
constexpr Status StatusRetinaInUse(StatusCodeRetinaInUse);
constexpr Status StatusCompressFailed(StatusCodeCompressFailed);
constexpr Status StatusDeCompressFailed(StatusCodeDeCompressFailed);
constexpr Status StatusQrQueryNameInvalid(StatusCodeQrQueryNameInvalid);
constexpr Status StatusQrQueryAlreadyDeleted(StatusCodeQrQueryAlreadyDeleted);
constexpr Status StatusQrQueryInUse(StatusCodeQrQueryInUse);
constexpr Status StatusXdbSerError(StatusCodeXdbSerError);
constexpr Status StatusXdbDesError(StatusCodeXdbDesError);
constexpr Status StatusXdbResident(StatusCodeXdbResident);
constexpr Status StatusXdbNotResident(StatusCodeXdbNotResident);
constexpr Status StatusSessionAlreadyInact(StatusCodeSessionAlreadyInact);
constexpr Status StatusSessionInact(StatusCodeSessionInact);
constexpr Status StatusSessionUsrAlreadyDeleted(
    StatusCodeSessionUsrAlreadyDeleted);
constexpr Status StatusSessionUsrNotExist(StatusCodeSessionUsrNotExist);
constexpr Status StatusNoShutdownPrivilege(StatusCodeNoShutdownPrivilege);
constexpr Status StatusSerializationListEmpty(StatusCodeSerializationListEmpty);
constexpr Status StatusAppAlreadyExists(StatusCodeAppAlreadyExists);
constexpr Status StatusAppNotFound(StatusCodeAppNotFound);
constexpr Status StatusAppInUse(StatusCodeAppInUse);
constexpr Status StatusInvalidStreamContext(StatusCodeInvalidStreamContext);
constexpr Status StatusInvalidStatsProtocol(StatusCodeInvalidStatsProtocol);
constexpr Status StatusStatStreamPartialFailure(
    StatusCodeStatStreamPartialFailure);
constexpr Status StatusLogLevelSetInvalid(StatusCodeLogLevelSetInvalid);
constexpr Status StatusConnectionWrongHandshake(
    StatusCodeConnectionWrongHandshake);
constexpr Status StatusQueryOnAnotherNode(StatusCodeQueryOnAnotherNode);
constexpr Status StatusAppInstanceStartError(StatusCodeAppInstanceStartError);
constexpr Status StatusApisRecvTimeout(StatusCodeApisRecvTimeout);
constexpr Status StatusIpAddrTooLong(StatusCodeIpAddrTooLong);
constexpr Status StatusSupportBundleNotSent(StatusCodeSupportBundleNotSent);
constexpr Status StatusInvalidBlobStreamProtocol(
    StatusCodeInvalidBlobStreamProtocol);
constexpr Status StatusStreamPartialFailure(StatusCodeStreamPartialFailure);
constexpr Status StatusUnknownProcMemInfoFileFormat(
    StatusCodeUnknownProcMemInfoFileFormat);
constexpr Status StatusApisWorkInvalidSignature(
    StatusCodeApisWorkInvalidSignature);
constexpr Status StatusApisWorkInvalidLength(StatusCodeApisWorkInvalidLength);
constexpr Status StatusLMDBError(StatusCodeLMDBError);
constexpr Status StatusXpuNoBufsToRecv(StatusCodeXpuNoBufsToRecv);
constexpr Status StatusJoinInvalidOrdering(StatusCodeJoinInvalidOrdering);
constexpr Status StatusDatasetAlreadyLocked(StatusCodeDatasetAlreadyLocked);
constexpr Status StatusUsrnodeStillAlive(StatusCodeUsrnodeStillAlive);
constexpr Status StatusBufferOnFailed(StatusCodeBufferOnFailed);
constexpr Status StatusCantUnbufferLogs(StatusCodeCantUnbufferLogs);
constexpr Status StatusLogFlushPeriodFailure(StatusCodeLogFlushPeriodFailure);
constexpr Status StatusInvalidLogLevel(StatusCodeInvalidLogLevel);
constexpr Status StatusNoDsUsers(StatusCodeNoDsUsers);
constexpr Status StatusJsonQueryParseError(StatusCodeJsonQueryParseError);
constexpr Status StatusXemNotConfigured(StatusCodeXemNotConfigured);
constexpr Status StatusNoDatasetMemory(StatusCodeNoDatasetMemory);
constexpr Status StatusTableEmpty(StatusCodeTableEmpty);
constexpr Status StatusUsrAddInProg(StatusCodeUsrAddInProg);
constexpr Status StatusSessionNotActive(StatusCodeSessionNotActive);
constexpr Status StatusUsrSessLoadFailed(StatusCodeUsrSessLoadFailed);
constexpr Status StatusProtobufError(StatusCodeProtobufError);
constexpr Status StatusRecordError(StatusCodeRecordError);
constexpr Status StatusCannotReplaceKey(StatusCodeCannotReplaceKey);
constexpr Status StatusSerializationIsDisabled(
    StatusCodeSerializationIsDisabled);
constexpr Status StatusFieldLimitExceeded(StatusCodeFieldLimitExceeded);
constexpr Status StatusWrongNumberOfArgs(StatusCodeWrongNumberOfArgs);
constexpr Status StatusMissingXcalarOpCode(StatusCodeMissingXcalarOpCode);
constexpr Status StatusMissingXcalarRankOver(StatusCodeMissingXcalarRankOver);
constexpr Status StatusInvalidXcalarOpCode(StatusCodeInvalidXcalarOpCode);
constexpr Status StatusInvalidXcalarRankOver(StatusCodeInvalidXcalarRankOver);
constexpr Status StatusInvalidRuntimeParams(StatusCodeInvalidRuntimeParams);
constexpr Status StatusInvPubTableName(StatusCodeInvPubTableName);
constexpr Status StatusExistsPubTableName(StatusCodeExistsPubTableName);
constexpr Status StatusUnlicensedFeatureInUse(StatusCodeUnlicensedFeatureInUse);
constexpr Status StatusLicPrivKeyMissing(StatusCodeLicPrivKeyMissing);
constexpr Status StatusLicPrivKeyErr(StatusCodeLicPrivKeyErr);
constexpr Status StatusLicPasswdMissing(StatusCodeLicPasswdMissing);
constexpr Status StatusLicLicenseMissing(StatusCodeLicLicenseMissing);
constexpr Status StatusLicSignatureMissing(StatusCodeLicSignatureMissing);
constexpr Status StatusLicBufTooSmall(StatusCodeLicBufTooSmall);
constexpr Status StatusLicPasswordError(StatusCodeLicPasswordError);
constexpr Status StatusLicValueOutOfRange(StatusCodeLicValueOutOfRange);
constexpr Status StatusLicDecompressInit(StatusCodeLicDecompressInit);
constexpr Status StatusLicDecompressErr(StatusCodeLicDecompressErr);
constexpr Status StatusLicLicenseTooLarge(StatusCodeLicLicenseTooLarge);
constexpr Status StatusLicCompressInit(StatusCodeLicCompressInit);
constexpr Status StatusLicUnsupportedOperation(
    StatusCodeLicUnsupportedOperation);
constexpr Status StatusLicOpDisabledUnlicensed(
    StatusCodeLicOpDisabledUnlicensed);
constexpr Status StatusWorkbookInvalidVersion(StatusCodeWorkbookInvalidVersion);
constexpr Status StatusPubTableNameNotFound(StatusCodePubTableNameNotFound);
constexpr Status StatusPubTableUpdateNotFound(StatusCodePubTableUpdateNotFound);
constexpr Status StatusUpgradeRequired(StatusCodeUpgradeRequired);
constexpr Status StatusUdfNotSupportedInCrossJoins(
    StatusCodeUdfNotSupportedInCrossJoins);
constexpr Status StatusRetinaNameInvalid(StatusCodeRetinaNameInvalid);
constexpr Status StatusNoSerDesPath(StatusCodeNoSerDesPath);
constexpr Status StatusEvalInvalidToken(StatusCodeEvalInvalidToken);
constexpr Status StatusXdfInvalidArrayInput(StatusCodeXdfInvalidArrayInput);
constexpr Status StatusBufCacheThickAllocFailed(
    StatusCodeBufCacheThickAllocFailed);
constexpr Status StatusDurBadSha(StatusCodeDurBadSha);
constexpr Status StatusDurBadIdlVer(StatusCodeDurBadIdlVer);
constexpr Status StatusSessListIncomplete(StatusCodeSessListIncomplete);
constexpr Status StatusPubTableInactive(StatusCodePubTableInactive);
constexpr Status StatusPubTableRestoring(StatusCodePubTableRestoring);
constexpr Status StatusSelfSelectRequired(StatusCodeSelfSelectRequired);
constexpr Status StatusSessionNameMissing(StatusCodeSessionNameMissing);
constexpr Status StatusXpuConnAborted(StatusCodeXpuConnAborted);
constexpr Status StatusPTUpdatePermDenied(StatusCodePTUpdatePermDenied);
constexpr Status StatusPTCoalescePermDenied(StatusCodePTCoalescePermDenied);
constexpr Status StatusPTOwnerNodeMismatch(StatusCodePTOwnerNodeMismatch);
constexpr Status StatusNsRefToObjectDenied(StatusCodeNsRefToObjectDenied);
constexpr Status StatusChecksumNotFound(StatusCodeChecksumNotFound);
constexpr Status StatusChecksumMismatch(StatusCodeChecksumMismatch);
constexpr Status StatusRuntimeSetParamInvalid(StatusCodeRuntimeSetParamInvalid);
constexpr Status StatusRuntimeSetParamNotSupported(
    StatusCodeRuntimeSetParamNotSupported);
constexpr Status StatusPublishTableSnapshotInProgress(
    StatusCodePublishTableSnapshotInProgress);
constexpr Status StatusSelectLimitReached(StatusCodeSelectLimitReached);
// XXX: following status not used; leave it here (deletion causes XD mis-match)
constexpr Status StatusUDFOwnerNodeMismatch(StatusCodeUDFOwnerNodeMismatch);
constexpr Status StatusUDFSourceMismatch(StatusCodeUDFSourceMismatch);
constexpr Status StatusUDFUpdateFailed(StatusCodeUDFUpdateFailed);
constexpr Status StatusUDFBadPath(StatusCodeUDFBadPath);
constexpr Status StatusDsMetaDataNotFound(StatusCodeDsMetaDataNotFound);
constexpr Status StatusDatasetAlreadyUnloaded(StatusCodeDatasetAlreadyUnloaded);
constexpr Status StatusUdfModuleFullNameRequired(
    StatusCodeUdfModuleFullNameRequired);
constexpr Status StatusCgroupsDisabled(StatusCodeCgroupsDisabled);
constexpr Status StatusCgroupAppInProgress(StatusCodeCgroupAppInProgress);
constexpr Status StatusJsonSessSerializeError(StatusCodeJsonSessSerializeError);
constexpr Status StatusSessMdataInconsistent(StatusCodeSessMdataInconsistent);
constexpr Status StatusRuntimeChangeInProgress(
    StatusCodeRuntimeChangeInProgress);
constexpr Status StatusLegacyTargetNotFound(StatusCodeLegacyTargetNotFound);
constexpr Status StatusDfpError(StatusCodeDfpError);
constexpr Status StatusCgroupInProgress(StatusCodeCgroupInProgress);
constexpr Status StatusKvInvalidKey(StatusCodeKvInvalidKey);
constexpr Status StatusKvInvalidValue(StatusCodeKvInvalidValue);
constexpr Status StatusComplexTypeNotSupported(
    StatusCodeComplexTypeNotSupported);
constexpr Status StatusParquetParserError(StatusCodeParquetParserError);
constexpr Status StatusUnSupportedDecimalType(StatusCodeUnSupportedDecimalType);
constexpr Status StatusUnSupportedLogicalType(StatusCodeUnSupportedLogicalType);
constexpr Status StatusKVSRefCountLeak(StatusCodeKVSRefCountLeak);
constexpr Status StatusTableNotPinned(StatusCodeTableNotPinned);
constexpr Status StatusTableAlreadyPinned(StatusCodeTableAlreadyPinned);
constexpr Status StatusTablePinned(StatusCodeTablePinned);
constexpr Status StatusDeferFreeXdbPageHdr(StatusCodeDeferFreeXdbPageHdr);
constexpr Status StatusDagNodeNumParentInvalid(
    StatusCodeDagNodeNumParentInvalid);
constexpr Status StatusInvTableName(StatusCodeInvTableName);
constexpr Status StatusExistsTableName(StatusCodeExistsTableName);
constexpr Status StatusTableNameNotFound(StatusCodeTableNameNotFound);
constexpr Status StatusMapFailureSummarySchema(
    StatusCodeMapFailureSummarySchema);
constexpr Status StatusMapFailureMultiEval(StatusCodeMapFailureMultiEval);
constexpr Status StatusUdfIcvModeFailure(StatusCodeUdfIcvModeFailure);
constexpr Status StatusColumnNameMismatch(StatusCodeColumnNameMismatch);
constexpr Status StatusTypeMismatch(StatusCodeTypeMismatch);
constexpr Status StatusKeyMismatch(StatusCodeKeyMismatch);
constexpr Status StatusKeyNameMismatch(StatusCodeKeyNameMismatch);
constexpr Status StatusKeyTypeMismatch(StatusCodeKeyTypeMismatch);
constexpr Status StatusDhtMismatch(StatusCodeDhtMismatch);
constexpr Status StatusDisallowedRankover(StatusCodeDisallowedRankover);
constexpr Status StatusDisallowedOpCode(StatusCodeDisallowedOpCode);
constexpr Status StatusDisallowedBatchId(StatusCodeDisallowedBatchId);
constexpr Status StatusDisallowedFatPtr(StatusCodeDisallowedFatPtr);
constexpr Status StatusInvalDeltaSchema(StatusCodeInvalDeltaSchema);
constexpr Status StatusInvalTableKeys(StatusCodeInvalTableKeys);
constexpr Status StatusClusterNotReady(StatusCodeClusterNotReady);
constexpr Status StatusNoCgroupCtrlPaths(StatusCodeNoCgroupCtrlPaths);
constexpr Status StatusCgroupCtrlPathLong(StatusCodeCgroupCtrlPathLong);
constexpr Status StatusOrderingMismatch(StatusCodeOrderingMismatch);
constexpr Status StatusIMDTableAlreadyLocked(StatusCodeIMDTableAlreadyLocked);
constexpr Status StatusExportMultipleTables(StatusCodeExportMultipleTables);
constexpr Status StatusColumnPositionMismatch(StatusCodeColumnPositionMismatch);
constexpr Status StatusXdbRefCountError(StatusCodeXdbRefCountError);
constexpr Status StatusStatsCollectionInProgress(
    StatusCodeStatsCollectionInProgress);
constexpr Status StatusTableIdNotFound(StatusCodeTableIdNotFound);
constexpr Status StatusInvalFullyQualTabName(StatusCodeInvalFullyQualTabName);
constexpr Status StatusUdfFlushFailed(StatusCodeUdfFlushFailed);
constexpr Status StatusTableNotGlobal(StatusCodeTableNotGlobal);
constexpr Status StatusFailedParseStrFieldLen(StatusCodeFailedParseStrFieldLen);
constexpr Status StatusFailedParseBoolField(StatusCodeFailedParseBoolField);
constexpr Status StatusFailedParseInt32Field(StatusCodeFailedParseInt32Field);
constexpr Status StatusFailedParseInt64Field(StatusCodeFailedParseInt64Field);
constexpr Status StatusFailedParseUint32Field(StatusCodeFailedParseUint32Field);
constexpr Status StatusFailedParseUint64Field(StatusCodeFailedParseUint64Field);
constexpr Status StatusFailedParseFloat32Field(
    StatusCodeFailedParseFloat32Field);
constexpr Status StatusFailedParseFloat64Field(
    StatusCodeFailedParseFloat64Field);
constexpr Status StatusFailedParseTimestampField(
    StatusCodeFailedParseTimestampField);
constexpr Status StatusFailedParseNumericField(
    StatusCodeFailedParseNumericField);
constexpr Status StatusFailedParseProtoValFieldLen(
    StatusCodeFailedParseProtoValFieldLen);
constexpr Status StatusFailedParseProtoValField(
    StatusCodeFailedParseProtoValField);
constexpr Status StatusInvalidFieldType(StatusCodeInvalidFieldType);
constexpr Status StatusSystemAppDisabled(StatusCodeSystemAppDisabled);
constexpr Status StatusUnionTypeMismatch(StatusCodeUnionTypeMismatch);
constexpr Status StatusUnionDhtMismatch(StatusCodeUnionDhtMismatch);
constexpr Status StatusAppInProgress(StatusCodeAppInProgress);
constexpr Status StatusMaxColumnsFound(StatusCodeMaxColumnsFound);
constexpr Status StatusXpuDeath(StatusCodeXpuDeath);
constexpr Status StatusScalarFunctionFieldOverflow(
    StatusCodeScalarFunctionFieldOverflow);
constexpr Status StatusUdfInvalidRetValue(StatusCodeUdfInvalidRetValue);
constexpr Status StatusUdfFuncAnnotErr(StatusCodeUdfFuncAnnotErr);
constexpr Status StatusUdfFuncRetBoolInvalid(StatusCodeUdfFuncRetBoolInvalid);
constexpr Status StatusUdfFuncRetStrInvalid(StatusCodeUdfFuncRetStrInvalid);
constexpr Status StatusUdfFuncRetInvalid(StatusCodeUdfFuncRetInvalid);

#endif  // _FSTATUS_H_
