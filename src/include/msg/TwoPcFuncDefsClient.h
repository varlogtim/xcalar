// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _TWOPCFUNCDEFSCLIENT_H_
#define _TWOPCFUNCDEFSCLIENT_H_

#include "msg/TwoPcFuncDefs.h"
#include "msg/MessageTypes.h"
#include "test/FuncTests/FuncTestDriver.h"

// Declarations here state that recvDataFromSocket() will deal with the
// work functions for these 2pc calls.
class TwoPcOp
{
  public:
    TwoPcOp() {}
    ~TwoPcOp() {}
    void init(MsgTypeId msgTypeId);
    MustCheck bool getRecvDataFromSocket();
    MustCheck bool getRecvDataFromSocketComp();
    MustCheck bool getFailIfResourceShortage();

  private:
    // Declarations here state that recvDataFromSocket() will deal with the
    // work functions for these 2pc calls.
    bool recvDataFromSocket_ = false;

    // Declarations here state that recvDataFromSocket() will deal with the
    // completion functions for these 2pc calls.
    bool recvDataFromSocketComp_ = false;

    // Typically 2PC aborts releases resources, so should not be rejected upon
    // resource shortage.
    bool failIfResourceShortage_ = false;
};

// Specialize for each TwoPcCallId.
class TwoPcAction
{
  public:
    TwoPcAction() {}
    virtual ~TwoPcAction() {}

    // Work function invoked on local or remote node when 2pc call is made.
    // This is the common case work function that most every call should use.
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload) = 0;

    // Completion function called right after work function only if message
    // is local.
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral,
                                      void *payload) = 0;

    // Invoked on source node after the receipt of a response (keep in mind
    // there could be responses from multiple remote nodes). Not called in
    // local case.
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload) = 0;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcNextResultSet1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcNextResultSet1() {}
    virtual ~TwoPcMsg2pcNextResultSet1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcNextResultSet1(const TwoPcMsg2pcNextResultSet1 &) = delete;
    TwoPcMsg2pcNextResultSet1(const TwoPcMsg2pcNextResultSet1 &&) = delete;
    TwoPcMsg2pcNextResultSet1 &operator=(const TwoPcMsg2pcNextResultSet1 &) =
        delete;
    TwoPcMsg2pcNextResultSet1 &operator=(const TwoPcMsg2pcNextResultSet1 &&) =
        delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcGetDataUsingFatptr1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcGetDataUsingFatptr1() {}
    virtual ~TwoPcMsg2pcGetDataUsingFatptr1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcGetDataUsingFatptr1(const TwoPcMsg2pcGetDataUsingFatptr1 &) =
        delete;
    TwoPcMsg2pcGetDataUsingFatptr1(const TwoPcMsg2pcGetDataUsingFatptr1 &&) =
        delete;
    TwoPcMsg2pcGetDataUsingFatptr1 &operator=(
        const TwoPcMsg2pcGetDataUsingFatptr1 &) = delete;
    TwoPcMsg2pcGetDataUsingFatptr1 &operator=(
        const TwoPcMsg2pcGetDataUsingFatptr1 &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcRpcGetStat1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcRpcGetStat1() {}
    virtual ~TwoPcMsg2pcRpcGetStat1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcRpcGetStat1(const TwoPcMsg2pcRpcGetStat1 &) = delete;
    TwoPcMsg2pcRpcGetStat1(const TwoPcMsg2pcRpcGetStat1 &&) = delete;
    TwoPcMsg2pcRpcGetStat1 &operator=(const TwoPcMsg2pcRpcGetStat1 &) = delete;
    TwoPcMsg2pcRpcGetStat1 &operator=(const TwoPcMsg2pcRpcGetStat1 &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcCountResultSetEntries1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcCountResultSetEntries1() {}
    virtual ~TwoPcMsg2pcCountResultSetEntries1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcCountResultSetEntries1(
        const TwoPcMsg2pcCountResultSetEntries1 &) = delete;
    TwoPcMsg2pcCountResultSetEntries1(
        const TwoPcMsg2pcCountResultSetEntries1 &&) = delete;
    TwoPcMsg2pcCountResultSetEntries1 &operator=(
        const TwoPcMsg2pcCountResultSetEntries1 &) = delete;
    TwoPcMsg2pcCountResultSetEntries1 &operator=(
        const TwoPcMsg2pcCountResultSetEntries1 &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcXcalarApiFilter1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcXcalarApiFilter1() {}
    virtual ~TwoPcMsg2pcXcalarApiFilter1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcXcalarApiFilter1(const TwoPcMsg2pcXcalarApiFilter1 &) = delete;
    TwoPcMsg2pcXcalarApiFilter1(const TwoPcMsg2pcXcalarApiFilter1 &&) = delete;
    TwoPcMsg2pcXcalarApiFilter1 &operator=(
        const TwoPcMsg2pcXcalarApiFilter1 &) = delete;
    TwoPcMsg2pcXcalarApiFilter1 &operator=(
        const TwoPcMsg2pcXcalarApiFilter1 &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcXcalarApiGroupBy1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcXcalarApiGroupBy1() {}
    virtual ~TwoPcMsg2pcXcalarApiGroupBy1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcXcalarApiGroupBy1(const TwoPcMsg2pcXcalarApiGroupBy1 &) = delete;
    TwoPcMsg2pcXcalarApiGroupBy1(const TwoPcMsg2pcXcalarApiGroupBy1 &&) =
        delete;
    TwoPcMsg2pcXcalarApiGroupBy1 &operator=(
        const TwoPcMsg2pcXcalarApiGroupBy1 &) = delete;
    TwoPcMsg2pcXcalarApiGroupBy1 &operator=(
        const TwoPcMsg2pcXcalarApiGroupBy1 &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcXcalarApiJoin1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcXcalarApiJoin1() {}
    virtual ~TwoPcMsg2pcXcalarApiJoin1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcXcalarApiJoin1(const TwoPcMsg2pcXcalarApiJoin1 &) = delete;
    TwoPcMsg2pcXcalarApiJoin1(const TwoPcMsg2pcXcalarApiJoin1 &&) = delete;
    TwoPcMsg2pcXcalarApiJoin1 &operator=(const TwoPcMsg2pcXcalarApiJoin1 &) =
        delete;
    TwoPcMsg2pcXcalarApiJoin1 &operator=(const TwoPcMsg2pcXcalarApiJoin1 &&) =
        delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcXcalarApiGetTableMeta1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcXcalarApiGetTableMeta1() {}
    virtual ~TwoPcMsg2pcXcalarApiGetTableMeta1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcXcalarApiGetTableMeta1(
        const TwoPcMsg2pcXcalarApiGetTableMeta1 &) = delete;
    TwoPcMsg2pcXcalarApiGetTableMeta1(
        const TwoPcMsg2pcXcalarApiGetTableMeta1 &&) = delete;
    TwoPcMsg2pcXcalarApiGetTableMeta1 &operator=(
        const TwoPcMsg2pcXcalarApiGetTableMeta1 &) = delete;
    TwoPcMsg2pcXcalarApiGetTableMeta1 &operator=(
        const TwoPcMsg2pcXcalarApiGetTableMeta1 &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcResetStat1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcResetStat1() {}
    virtual ~TwoPcMsg2pcResetStat1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcResetStat1(const TwoPcMsg2pcResetStat1 &) = delete;
    TwoPcMsg2pcResetStat1(const TwoPcMsg2pcResetStat1 &&) = delete;
    TwoPcMsg2pcResetStat1 &operator=(const TwoPcMsg2pcResetStat1 &) = delete;
    TwoPcMsg2pcResetStat1 &operator=(const TwoPcMsg2pcResetStat1 &&) = delete;
};

class TwoPcMsg2pcLogLevelSet1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcLogLevelSet1() {}
    virtual ~TwoPcMsg2pcLogLevelSet1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcLogLevelSet1(const TwoPcMsg2pcLogLevelSet1 &) = delete;
    TwoPcMsg2pcLogLevelSet1(const TwoPcMsg2pcLogLevelSet1 &&) = delete;
    TwoPcMsg2pcLogLevelSet1 &operator=(const TwoPcMsg2pcLogLevelSet1 &) =
        delete;
    TwoPcMsg2pcLogLevelSet1 &operator=(const TwoPcMsg2pcLogLevelSet1 &&) =
        delete;
};

class TwoPcMsg2pcSetConfig1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcSetConfig1() {}
    virtual ~TwoPcMsg2pcSetConfig1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcSetConfig1(const TwoPcMsg2pcSetConfig1 &) = delete;
    TwoPcMsg2pcSetConfig1(const TwoPcMsg2pcSetConfig1 &&) = delete;
    TwoPcMsg2pcSetConfig1 &operator=(const TwoPcMsg2pcSetConfig1 &) = delete;
    TwoPcMsg2pcSetConfig1 &operator=(const TwoPcMsg2pcSetConfig1 &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcXcalarApiIndex1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcXcalarApiIndex1() {}
    virtual ~TwoPcMsg2pcXcalarApiIndex1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcXcalarApiIndex1(const TwoPcMsg2pcXcalarApiIndex1 &) = delete;
    TwoPcMsg2pcXcalarApiIndex1(const TwoPcMsg2pcXcalarApiIndex1 &&) = delete;
    TwoPcMsg2pcXcalarApiIndex1 &operator=(const TwoPcMsg2pcXcalarApiIndex1 &) =
        delete;
    TwoPcMsg2pcXcalarApiIndex1 &operator=(const TwoPcMsg2pcXcalarApiIndex1 &&) =
        delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcXcalarApiSynthesize1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcXcalarApiSynthesize1() {}
    virtual ~TwoPcMsg2pcXcalarApiSynthesize1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcXcalarApiSynthesize1(const TwoPcMsg2pcXcalarApiSynthesize1 &) =
        delete;
    TwoPcMsg2pcXcalarApiSynthesize1(const TwoPcMsg2pcXcalarApiSynthesize1 &&) =
        delete;
    TwoPcMsg2pcXcalarApiSynthesize1 &operator=(
        const TwoPcMsg2pcXcalarApiSynthesize1 &) = delete;
    TwoPcMsg2pcXcalarApiSynthesize1 &operator=(
        const TwoPcMsg2pcXcalarApiSynthesize1 &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcGetStatGroupIdMap1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcGetStatGroupIdMap1() {}
    virtual ~TwoPcMsg2pcGetStatGroupIdMap1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcGetStatGroupIdMap1(const TwoPcMsg2pcGetStatGroupIdMap1 &) =
        delete;
    TwoPcMsg2pcGetStatGroupIdMap1(const TwoPcMsg2pcGetStatGroupIdMap1 &&) =
        delete;
    TwoPcMsg2pcGetStatGroupIdMap1 &operator=(
        const TwoPcMsg2pcGetStatGroupIdMap1 &) = delete;
    TwoPcMsg2pcGetStatGroupIdMap1 &operator=(
        const TwoPcMsg2pcGetStatGroupIdMap1 &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcGetKeysUsingFatptr1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcGetKeysUsingFatptr1() {}
    virtual ~TwoPcMsg2pcGetKeysUsingFatptr1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcGetKeysUsingFatptr1(const TwoPcMsg2pcGetKeysUsingFatptr1 &) =
        delete;
    TwoPcMsg2pcGetKeysUsingFatptr1(const TwoPcMsg2pcGetKeysUsingFatptr1 &&) =
        delete;
    TwoPcMsg2pcGetKeysUsingFatptr1 &operator=(
        const TwoPcMsg2pcGetKeysUsingFatptr1 &) = delete;
    TwoPcMsg2pcGetKeysUsingFatptr1 &operator=(
        const TwoPcMsg2pcGetKeysUsingFatptr1 &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcXcalarApiMap1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcXcalarApiMap1() {}
    virtual ~TwoPcMsg2pcXcalarApiMap1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcXcalarApiMap1(const TwoPcMsg2pcXcalarApiMap1 &) = delete;
    TwoPcMsg2pcXcalarApiMap1(const TwoPcMsg2pcXcalarApiMap1 &&) = delete;
    TwoPcMsg2pcXcalarApiMap1 &operator=(const TwoPcMsg2pcXcalarApiMap1 &) =
        delete;
    TwoPcMsg2pcXcalarApiMap1 &operator=(const TwoPcMsg2pcXcalarApiMap1 &&) =
        delete;
};

class TwoPcMsg2pcXcalarApiUnion1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcXcalarApiUnion1() {}
    virtual ~TwoPcMsg2pcXcalarApiUnion1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcXcalarApiUnion1(const TwoPcMsg2pcXcalarApiUnion1 &) = delete;
    TwoPcMsg2pcXcalarApiUnion1(const TwoPcMsg2pcXcalarApiUnion1 &&) = delete;
    TwoPcMsg2pcXcalarApiUnion1 &operator=(const TwoPcMsg2pcXcalarApiUnion1 &) =
        delete;
    TwoPcMsg2pcXcalarApiUnion1 &operator=(const TwoPcMsg2pcXcalarApiUnion1 &&) =
        delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcXcalarApiTop1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcXcalarApiTop1() {}
    virtual ~TwoPcMsg2pcXcalarApiTop1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcXcalarApiTop1(const TwoPcMsg2pcXcalarApiTop1 &) = delete;
    TwoPcMsg2pcXcalarApiTop1(const TwoPcMsg2pcXcalarApiTop1 &&) = delete;
    TwoPcMsg2pcXcalarApiTop1 &operator=(const TwoPcMsg2pcXcalarApiTop1 &) =
        delete;
    TwoPcMsg2pcXcalarApiTop1 &operator=(const TwoPcMsg2pcXcalarApiTop1 &&) =
        delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcXdfParallelDo1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcXdfParallelDo1() {}
    virtual ~TwoPcMsg2pcXdfParallelDo1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcXdfParallelDo1(const TwoPcMsg2pcXdfParallelDo1 &) = delete;
    TwoPcMsg2pcXdfParallelDo1(const TwoPcMsg2pcXdfParallelDo1 &&) = delete;
    TwoPcMsg2pcXdfParallelDo1 &operator=(const TwoPcMsg2pcXdfParallelDo1 &) =
        delete;
    TwoPcMsg2pcXdfParallelDo1 &operator=(const TwoPcMsg2pcXdfParallelDo1 &&) =
        delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcXcalarApiProject1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcXcalarApiProject1() {}
    virtual ~TwoPcMsg2pcXcalarApiProject1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcXcalarApiProject1(const TwoPcMsg2pcXcalarApiProject1 &) = delete;
    TwoPcMsg2pcXcalarApiProject1(const TwoPcMsg2pcXcalarApiProject1 &&) =
        delete;
    TwoPcMsg2pcXcalarApiProject1 &operator=(
        const TwoPcMsg2pcXcalarApiProject1 &) = delete;
    TwoPcMsg2pcXcalarApiProject1 &operator=(
        const TwoPcMsg2pcXcalarApiProject1 &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcDropXdb1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcDropXdb1() {}
    virtual ~TwoPcMsg2pcDropXdb1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcDropXdb1(const TwoPcMsg2pcDropXdb1 &) = delete;
    TwoPcMsg2pcDropXdb1(const TwoPcMsg2pcDropXdb1 &&) = delete;
    TwoPcMsg2pcDropXdb1 &operator=(const TwoPcMsg2pcDropXdb1 &) = delete;
    TwoPcMsg2pcDropXdb1 &operator=(const TwoPcMsg2pcDropXdb1 &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcDeserializeXdbPages1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcDeserializeXdbPages1() {}
    virtual ~TwoPcMsg2pcDeserializeXdbPages1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcDeserializeXdbPages1(const TwoPcMsg2pcDeserializeXdbPages1 &) =
        delete;
    TwoPcMsg2pcDeserializeXdbPages1(const TwoPcMsg2pcDeserializeXdbPages1 &&) =
        delete;
    TwoPcMsg2pcDeserializeXdbPages1 &operator=(
        const TwoPcMsg2pcDeserializeXdbPages1 &) = delete;
    TwoPcMsg2pcDeserializeXdbPages1 &operator=(
        const TwoPcMsg2pcDeserializeXdbPages1 &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcSerializeXdb : public TwoPcAction
{
  public:
    TwoPcMsg2pcSerializeXdb() {}
    virtual ~TwoPcMsg2pcSerializeXdb() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcSerializeXdb(const TwoPcMsg2pcSerializeXdb &) = delete;
    TwoPcMsg2pcSerializeXdb(const TwoPcMsg2pcSerializeXdb &&) = delete;
    TwoPcMsg2pcSerializeXdb &operator=(const TwoPcMsg2pcSerializeXdb &) =
        delete;
    TwoPcMsg2pcSerializeXdb &operator=(const TwoPcMsg2pcSerializeXdb &&) =
        delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcDeserializeXdb : public TwoPcAction
{
  public:
    TwoPcMsg2pcDeserializeXdb() {}
    virtual ~TwoPcMsg2pcDeserializeXdb() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcDeserializeXdb(const TwoPcMsg2pcDeserializeXdb &) = delete;
    TwoPcMsg2pcDeserializeXdb(const TwoPcMsg2pcDeserializeXdb &&) = delete;
    TwoPcMsg2pcDeserializeXdb &operator=(const TwoPcMsg2pcDeserializeXdb &) =
        delete;
    TwoPcMsg2pcDeserializeXdb &operator=(const TwoPcMsg2pcDeserializeXdb &&) =
        delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcQueryState1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcQueryState1() {}
    virtual ~TwoPcMsg2pcQueryState1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcQueryState1(const TwoPcMsg2pcQueryState1 &) = delete;
    TwoPcMsg2pcQueryState1(const TwoPcMsg2pcQueryState1 &&) = delete;
    TwoPcMsg2pcQueryState1 &operator=(const TwoPcMsg2pcQueryState1 &) = delete;
    TwoPcMsg2pcQueryState1 &operator=(const TwoPcMsg2pcQueryState1 &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcXdbLoadDone1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcXdbLoadDone1() {}
    virtual ~TwoPcMsg2pcXdbLoadDone1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcXdbLoadDone1(const TwoPcMsg2pcXdbLoadDone1 &) = delete;
    TwoPcMsg2pcXdbLoadDone1(const TwoPcMsg2pcXdbLoadDone1 &&) = delete;
    TwoPcMsg2pcXdbLoadDone1 &operator=(const TwoPcMsg2pcXdbLoadDone1 &) =
        delete;
    TwoPcMsg2pcXdbLoadDone1 &operator=(const TwoPcMsg2pcXdbLoadDone1 &&) =
        delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcDlmResolveKeyType1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcDlmResolveKeyType1() {}
    virtual ~TwoPcMsg2pcDlmResolveKeyType1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcDlmResolveKeyType1(const TwoPcMsg2pcDlmResolveKeyType1 &) =
        delete;
    TwoPcMsg2pcDlmResolveKeyType1(const TwoPcMsg2pcDlmResolveKeyType1 &&) =
        delete;
    TwoPcMsg2pcDlmResolveKeyType1 &operator=(
        const TwoPcMsg2pcDlmResolveKeyType1 &) = delete;
    TwoPcMsg2pcDlmResolveKeyType1 &operator=(
        const TwoPcMsg2pcDlmResolveKeyType1 &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcExportMakeScalarTable1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcExportMakeScalarTable1() {}
    virtual ~TwoPcMsg2pcExportMakeScalarTable1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcExportMakeScalarTable1(
        const TwoPcMsg2pcExportMakeScalarTable1 &) = delete;
    TwoPcMsg2pcExportMakeScalarTable1(
        const TwoPcMsg2pcExportMakeScalarTable1 &&) = delete;
    TwoPcMsg2pcExportMakeScalarTable1 &operator=(
        const TwoPcMsg2pcExportMakeScalarTable1 &) = delete;
    TwoPcMsg2pcExportMakeScalarTable1 &operator=(
        const TwoPcMsg2pcExportMakeScalarTable1 &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcTwoPcBarrier : public TwoPcAction
{
  public:
    TwoPcMsg2pcTwoPcBarrier() {}
    virtual ~TwoPcMsg2pcTwoPcBarrier() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcTwoPcBarrier(const TwoPcMsg2pcTwoPcBarrier &) = delete;
    TwoPcMsg2pcTwoPcBarrier(const TwoPcMsg2pcTwoPcBarrier &&) = delete;
    TwoPcMsg2pcTwoPcBarrier &operator=(const TwoPcMsg2pcTwoPcBarrier &) =
        delete;
    TwoPcMsg2pcTwoPcBarrier &operator=(const TwoPcMsg2pcTwoPcBarrier &&) =
        delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcXcalarAggregateEvalDistributeEvalFn1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcXcalarAggregateEvalDistributeEvalFn1() {}
    virtual ~TwoPcMsg2pcXcalarAggregateEvalDistributeEvalFn1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcXcalarAggregateEvalDistributeEvalFn1(
        const TwoPcMsg2pcXcalarAggregateEvalDistributeEvalFn1 &) = delete;
    TwoPcMsg2pcXcalarAggregateEvalDistributeEvalFn1(
        const TwoPcMsg2pcXcalarAggregateEvalDistributeEvalFn1 &&) = delete;
    TwoPcMsg2pcXcalarAggregateEvalDistributeEvalFn1 &operator=(
        const TwoPcMsg2pcXcalarAggregateEvalDistributeEvalFn1 &) = delete;
    TwoPcMsg2pcXcalarAggregateEvalDistributeEvalFn1 &operator=(
        const TwoPcMsg2pcXcalarAggregateEvalDistributeEvalFn1 &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcTestImmed1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcTestImmed1() {}
    virtual ~TwoPcMsg2pcTestImmed1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcTestImmed1(const TwoPcMsg2pcTestImmed1 &) = delete;
    TwoPcMsg2pcTestImmed1(const TwoPcMsg2pcTestImmed1 &&) = delete;
    TwoPcMsg2pcTestImmed1 &operator=(const TwoPcMsg2pcTestImmed1 &) = delete;
    TwoPcMsg2pcTestImmed1 &operator=(const TwoPcMsg2pcTestImmed1 &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcTestRecv1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcTestRecv1() {}
    virtual ~TwoPcMsg2pcTestRecv1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcTestRecv1(const TwoPcMsg2pcTestRecv1 &) = delete;
    TwoPcMsg2pcTestRecv1(const TwoPcMsg2pcTestRecv1 &&) = delete;
    TwoPcMsg2pcTestRecv1 &operator=(const TwoPcMsg2pcTestRecv1 &) = delete;
    TwoPcMsg2pcTestRecv1 &operator=(const TwoPcMsg2pcTestRecv1 &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcTestImmedFuncTest : public TwoPcAction
{
  public:
    TwoPcMsg2pcTestImmedFuncTest() {}
    virtual ~TwoPcMsg2pcTestImmedFuncTest() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcTestImmedFuncTest(const TwoPcMsg2pcTestImmedFuncTest &) = delete;
    TwoPcMsg2pcTestImmedFuncTest(const TwoPcMsg2pcTestImmedFuncTest &&) =
        delete;
    TwoPcMsg2pcTestImmedFuncTest &operator=(
        const TwoPcMsg2pcTestImmedFuncTest &) = delete;
    TwoPcMsg2pcTestImmedFuncTest &operator=(
        const TwoPcMsg2pcTestImmedFuncTest &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcTestRecvFuncTest : public TwoPcAction
{
  public:
    TwoPcMsg2pcTestRecvFuncTest() {}
    virtual ~TwoPcMsg2pcTestRecvFuncTest() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcTestRecvFuncTest(const TwoPcMsg2pcTestRecvFuncTest &) = delete;
    TwoPcMsg2pcTestRecvFuncTest(const TwoPcMsg2pcTestRecvFuncTest &&) = delete;
    TwoPcMsg2pcTestRecvFuncTest &operator=(
        const TwoPcMsg2pcTestRecvFuncTest &) = delete;
    TwoPcMsg2pcTestRecvFuncTest &operator=(
        const TwoPcMsg2pcTestRecvFuncTest &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcDlmUpdateKvStore1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcDlmUpdateKvStore1() {}
    virtual ~TwoPcMsg2pcDlmUpdateKvStore1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcDlmUpdateKvStore1(const TwoPcMsg2pcDlmUpdateKvStore1 &) = delete;
    TwoPcMsg2pcDlmUpdateKvStore1(const TwoPcMsg2pcDlmUpdateKvStore1 &&) =
        delete;
    TwoPcMsg2pcDlmUpdateKvStore1 &operator=(
        const TwoPcMsg2pcDlmUpdateKvStore1 &) = delete;
    TwoPcMsg2pcDlmUpdateKvStore1 &operator=(
        const TwoPcMsg2pcDlmUpdateKvStore1 &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcXcalarApiAggregate1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcXcalarApiAggregate1() {}
    virtual ~TwoPcMsg2pcXcalarApiAggregate1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcXcalarApiAggregate1(const TwoPcMsg2pcXcalarApiAggregate1 &) =
        delete;
    TwoPcMsg2pcXcalarApiAggregate1(const TwoPcMsg2pcXcalarApiAggregate1 &&) =
        delete;
    TwoPcMsg2pcXcalarApiAggregate1 &operator=(
        const TwoPcMsg2pcXcalarApiAggregate1 &) = delete;
    TwoPcMsg2pcXcalarApiAggregate1 &operator=(
        const TwoPcMsg2pcXcalarApiAggregate1 &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcXcalarApiGetRowNum1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcXcalarApiGetRowNum1() {}
    virtual ~TwoPcMsg2pcXcalarApiGetRowNum1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcXcalarApiGetRowNum1(const TwoPcMsg2pcXcalarApiGetRowNum1 &) =
        delete;
    TwoPcMsg2pcXcalarApiGetRowNum1(const TwoPcMsg2pcXcalarApiGetRowNum1 &&) =
        delete;
    TwoPcMsg2pcXcalarApiGetRowNum1 &operator=(
        const TwoPcMsg2pcXcalarApiGetRowNum1 &) = delete;
    TwoPcMsg2pcXcalarApiGetRowNum1 &operator=(
        const TwoPcMsg2pcXcalarApiGetRowNum1 &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcXcalarApiCancel1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcXcalarApiCancel1() {}
    virtual ~TwoPcMsg2pcXcalarApiCancel1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcXcalarApiCancel1(const TwoPcMsg2pcXcalarApiCancel1 &) = delete;
    TwoPcMsg2pcXcalarApiCancel1(const TwoPcMsg2pcXcalarApiCancel1 &&) = delete;
    TwoPcMsg2pcXcalarApiCancel1 &operator=(
        const TwoPcMsg2pcXcalarApiCancel1 &) = delete;
    TwoPcMsg2pcXcalarApiCancel1 &operator=(
        const TwoPcMsg2pcXcalarApiCancel1 &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcXcalarApiGetOpStatus1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcXcalarApiGetOpStatus1() {}
    virtual ~TwoPcMsg2pcXcalarApiGetOpStatus1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcXcalarApiGetOpStatus1(const TwoPcMsg2pcXcalarApiGetOpStatus1 &) =
        delete;
    TwoPcMsg2pcXcalarApiGetOpStatus1(
        const TwoPcMsg2pcXcalarApiGetOpStatus1 &&) = delete;
    TwoPcMsg2pcXcalarApiGetOpStatus1 &operator=(
        const TwoPcMsg2pcXcalarApiGetOpStatus1 &) = delete;
    TwoPcMsg2pcXcalarApiGetOpStatus1 &operator=(
        const TwoPcMsg2pcXcalarApiGetOpStatus1 &&) = delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcQueryCancel1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcQueryCancel1() {}
    virtual ~TwoPcMsg2pcQueryCancel1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcQueryCancel1(const TwoPcMsg2pcQueryCancel1 &) = delete;
    TwoPcMsg2pcQueryCancel1(const TwoPcMsg2pcQueryCancel1 &&) = delete;
    TwoPcMsg2pcQueryCancel1 &operator=(const TwoPcMsg2pcQueryCancel1 &) =
        delete;
    TwoPcMsg2pcQueryCancel1 &operator=(const TwoPcMsg2pcQueryCancel1 &&) =
        delete;
};

// XXX Move this to the sub-system
class TwoPcMsg2pcQueryDelete1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcQueryDelete1() {}
    virtual ~TwoPcMsg2pcQueryDelete1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcQueryDelete1(const TwoPcMsg2pcQueryDelete1 &) = delete;
    TwoPcMsg2pcQueryDelete1(const TwoPcMsg2pcQueryDelete1 &&) = delete;
    TwoPcMsg2pcQueryDelete1 &operator=(const TwoPcMsg2pcQueryDelete1 &) =
        delete;
    TwoPcMsg2pcQueryDelete1 &operator=(const TwoPcMsg2pcQueryDelete1 &&) =
        delete;
};

class TwoPcMsg2pcFuncTest1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcFuncTest1() {}
    virtual ~TwoPcMsg2pcFuncTest1() {}
    void schedLocalWork(MsgEphemeral *ephemeral, void *payload) override;
    void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload) override;
    void recvDataCompletion(MsgEphemeral *ephemeral, void *payload) override;

  private:
    TwoPcMsg2pcFuncTest1(const TwoPcMsg2pcFuncTest1 &) = delete;
    TwoPcMsg2pcFuncTest1 &operator=(const TwoPcMsg2pcFuncTest1 &) = delete;
};

class TwoPcMsg2pcGetRows1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcGetRows1() {}
    virtual ~TwoPcMsg2pcGetRows1() {}
    void schedLocalWork(MsgEphemeral *ephemeral, void *payload) override;
    void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload) override;
    void recvDataCompletion(MsgEphemeral *ephemeral, void *payload) override;

  private:
    TwoPcMsg2pcGetRows1(const TwoPcMsg2pcGetRows1 &) = delete;
    TwoPcMsg2pcGetRows1 &operator=(const TwoPcMsg2pcGetRows1 &) = delete;
};

#endif  // _TWOPCFUNCDEFSCLIENT_H_
