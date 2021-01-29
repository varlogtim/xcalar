// Copyright 2017 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _LIBNS_H_
#define _LIBNS_H_

#include "primitives/Primitives.h"
#include "runtime/Spinlock.h"
#include "operators/GenericTypes.h"
#include "util/IntHashTable.h"
#include "util/StringHashTable.h"
#include "hash/Hash.h"
#include "msg/TwoPcFuncDefsClient.h"
#include "ns/LibNsTypes.h"
#include "msg/Message.h"
#include "stat/Statistics.h"

class NsObject;

// This singleton class manages NsObjects and its derived classes.

class LibNs final
{
  private:
    struct NsOpOutput {
        char pathName[LibNsTypes::MaxPathNameLen];
        LibNsTypes::NsId nsId;
        uint64_t refCount;
        LibNsTypes::NsHandle nsHandle;
    };

    struct NsOpResponse {
        NsOpOutput opOutput;
        size_t nsObjectSize;
        // Optional, depends on Op
        uint8_t nsObject[];
    };

  public:
    // ======================================================================
    // The basic flow when using this library is:
    //
    //      publish()
    //      open()
    //      getNsObject()
    //      updateNsObject()
    //      close()
    //      remove()
    //
    //      Handles returned by open() can be passed around knowing that the
    //      underlying path name and associated object cannot be deleted from
    //      underneath.  If multiple entities need this protection they each
    //      must open the path name / NsId.
    //
    //      remove() will delete the path name and associated object if there
    //      are no opens.  If there are opens then the path name is marked for
    //      deletion and will be deleted when all the opens are closed.  During
    //      this time operations such as open will fail with
    //      StatusPendingDelete.
    //
    //      remove() and close() provide a indicator of whether or not the path
    //      name and object were deleted as a result of the remove/close.  This
    //      is useful for users who are maintaining a entity outside of LibNs
    //      but is associated with the name (e.g. DHTs or Datasets replicated
    //      in GVM).
    //
    //      For guaranteed remove semantics (remove either deletes the object
    //      or, if there are opens, it doesn't mark for deletion but instead
    //      "fails") one would open with read-write.  If that succeeds there
    //      are no other opens otherwise the read-write open would fail) and
    //      remove and then close.

    // ======================================================================
    // All of the following function calls involve a twoPc to the "dlm" node.
    // ======================================================================

    // Publish a fully qualified, hierarchical name into the global
    // namespace and associate with the name a client-specified object
    // derived from the NsObject base class.  If the NsId returned
    // is NsInvalidId then status provides the reason (e.g.
    // StatusExist).
    //
    // Either the path name or the NsId can be used from any node in the
    // cluster to open and access the object.

    MustCheck LibNsTypes::NsId publish(const char *pathName,
                                       NsObject *nsObj,
                                       Status *status);

    // Publish a fully qualified, hierarchical name into the global
    // namespace and associate with the name a client-specified object.
    // If the NsId returned is NsInvalidId then status provides the reason.
    //
    // The object will be removed when a close transitions the reference
    // count from 2 to 1...an explicit remove is not needed.  This is used
    // by clients which publish an object and pass around the ID doing opens
    // and closes with nothing knowing when the "last" close occurs
    // (e.g. open/open/close/open/close/close).

    MustCheck LibNsTypes::NsId publishWithAutoRemove(const char *pathName,
                                                     NsObject *nsObj,
                                                     Status *status);

    // Publishes a fully qualified, hierarchical name into the global
    // namespace.  An NsId is returned to the caller.  If the NsId returned
    // is NsInvalidId then status provides the reason (e.g.
    // StatusNsObjAlreadyExists).
    //
    // As no NsObject is provided a minimal one will be created and
    // associated with the path name.  The object can be updated at a later
    // time with a client-specified object.

    MustCheck LibNsTypes::NsId publish(const char *pathName, Status *status);

    // Opening a path name/NsId is subject to the following:
    //
    //      * Only one open with ReadWrite access is allowed at a time.
    //      * Open with ReadWrite access will fail if there are existing opens
    //        with ReadOnly access.
    //      * Open with ReadOnly access will fail if there is an existing
    //        open with ReadWrite access.
    //
    // Opens the path name/NsId and returns a handle which can be used to get
    // or update (if ReadWrite access) the associated object.

    MustCheck LibNsTypes::NsHandle open(const char *pathName,
                                        LibNsTypes::NsOpenFlags openFlags,
                                        Status *status);

    MustCheck LibNsTypes::NsHandle open(LibNsTypes::NsId nsId,
                                        LibNsTypes::NsOpenFlags openFlags,
                                        Status *status);

    // Opens the path name/NsId and returns a handle which can be used to get
    // or update (if ReadWrite access) the associated object.  A copy of the
    // object is also returned as a convenience (also saves a twoPc call).  The
    // caller is responsible for freeing the memory of the returned object.

    MustCheck LibNsTypes::NsHandle open(const char *pathName,
                                        LibNsTypes::NsOpenFlags openFlags,
                                        NsObject **nsObject,
                                        Status *status);

    MustCheck LibNsTypes::NsHandle open(LibNsTypes::NsId nsId,
                                        LibNsTypes::NsOpenFlags openFlags,
                                        NsObject **nsObject,
                                        Status *status);

    // Returns a copy of the object associated with the specified NsHandle.
    // If the specified handle is not found then NULL is returned.
    //
    // The caller is responsible for freeing the memory of the returned object.

    MustCheck NsObject *getNsObject(LibNsTypes::NsHandle nsHandle);

    // Returns a copy of the object associated with the specified pathName.
    // The caller is responsible for freeing the memory of the returned object.

    MustCheck NsObject *getNsObject(const char *pathName);

    // Updates the object associated with the handle to the specified new
    // object and return an updated nsHandle.  The handle must have been
    // returned by open() with ReadWrite flags.

    MustCheck LibNsTypes::NsHandle updateNsObject(LibNsTypes::NsHandle nsHandle,
                                                  NsObject *nsObj,
                                                  Status *status);

    // Returns the fully qualified, hierarchical name from the global
    // namespace associated with the specified NsId.  If the path name is
    // not found then NULL is returned.  The caller is responsible for
    // freeing the memory.

    MustCheck char *getPathName(LibNsTypes::NsId nsId);

    // Returns the NsId associated with the path name.  If the specified
    // path name is not found then NsInvalidId is returned.

    MustCheck LibNsTypes::NsId getNsId(const char *pathName);

    // Obtain a list of path info (name/Xid) matching the specified pattern.
    // The return value is the number of path names returned.  The caller is
    // responsible for freeing the memory.
    // This method requires a remote node twoPc to each node of the cluster.

    MustCheck size_t getPathInfo(const char *pattern,
                                 LibNsTypes::PathInfoOut **pathInfo);

    // Closes the specified NsHandle.  Returns whether or not the object
    // associated with the handle was deleted as a result of the close (NULL
    // can be specified).

    MustCheck Status close(LibNsTypes::NsHandle nsHandle, bool *objDeleted);

    // Remove when there's no longer any opens...
    //
    // Marking a path name and its associated object for removal means that
    // certain subsequent operations on that object will fail with a
    // StatusPendingRemoval error.  These operations include: open, getNsObject,
    // updateNsObject, getNsId, getPathName.
    //
    // When all opens on the path name / Nsid are closed the path name and
    // associated object are removed.

    // Prevents further operations (see above) on the specified path name/NsId.
    // Returns whether or not the object associated with the path name/NsId
    // was deleted as a result of the remove (NULL can be specified).

    MustCheck Status remove(const char *pathName, bool *objDeleted);
    MustCheck Status remove(LibNsTypes::NsId nsId, bool *objDeleted);

    // Does a remove() for all path names matching the specified pattern.
    // This requires a remote node twoPc to each node of the cluster.

    MustCheck Status removeMatching(const char *pattern);

    // Return the object placement NodeId given a pathName.
    MustCheck NodeId getNodeId(const char *pathName, Status *retStatus);

    // ======================================================================
    // End of function calls involving a twoPc to the "dlm" node.
    // ======================================================================

    static MustCheck Status init();
    void destroy();
    MustCheck static LibNs *get();

    // Used by message/dlm infrastructure.
    void dlmUpdateNsCompletion(MsgEphemeral *eph, void *payload);
    void updateDlm(MsgEphemeral *ephIn, void *payload);

    // Used by LibNs test
    MustCheck bool isValidPathName(const char *pathName, NodeId *retNodeId);

    // Make the handle "invalid" so that if it is mistakenly used it
    // will error out.
    void setHandleInvalid(LibNsTypes::NsHandle *nsHandle);

    // Used by stream handling.  Adds a chunk of the getPathInfo results
    // onto the received list.
    MustCheck Status addStreamData(void *ephemeral,
                                   void *data,
                                   size_t dataSize);

    MustCheck bool fqnHasNodeIdPrefix(char *fqn);

    // For a given nodeId and fqn, set the fully qualified name which
    // includes the nodeId required for the placement of object on a particular
    // node in the cluster.
    MustCheck Status setFQNForNodeId(NodeId nodeId, char *fqn, size_t fqnSize);

    // If the 'fqn' has a NodeId prefix, then strip it
    MustCheck Status setFQNStripNodeId(char *fqn, size_t fqnSize);

    // Given a nsHandle, return pointer to the underlying LibNs object. Note
    // that this call can fail if any of the below is true.
    // * If the LibNs object is not node local
    // * If nsHandle open flags is not ReadWriteExcl.
    MustCheck NsObject *getRefToObject(LibNsTypes::NsHandle nsHandle,
                                       Status *retStatus);

  private:
    static constexpr const char *moduleName = "libns";
    static constexpr const char *NodeIdPrefix = "/nodeId-";
    static LibNs *instance;

    // The types of LibNs messages sent via twoPc
    enum NsDlmOps {
        NsPublish,
        NsOpen,
        NsClose,
        NsGetInfo,
        NsUpdate,
        NsGetPathInfo,
        NsRemove,
    };

    // Header for each LibNs message
    struct NsHeader {
        LibNsTypes::NsId nsId;
        char pathName[LibNsTypes::MaxPathNameLen];
        // Use nsId vs pathName
        bool viaId;
        NsDlmOps nsOp;
    };

    // Publish a pathname with the associated object into the namespace.
    struct NsPublishMsg {
        NsHeader header;
        LibNsTypes::NsAutoRemove autoRemove;
        size_t nsObjSize;
        uint8_t nsObject[];
    };

    // Update the object.  Must have a read-write handle.
    struct NsUpdateMsg {
        NsHeader header;
        LibNsTypes::NsHandle nsHandle;
        size_t nsObjSize;
        uint8_t nsObject[];
    };

    // Open the specified path name or NsId
    struct NsOpenMsg {
        NsHeader header;
        LibNsTypes::NsOpenFlags openFlags;
        bool includeObject;
    };

    // Get info on the object specified by either path name or NsId.
    struct NsGetInfoMsg {
        NsHeader header;
        LibNsTypes::NsHandle nsHandle;
        bool includeObject;
    };

    // Get path info for all path names matching the pattern.
    struct NsGetPathInfoMsg {
        NsHeader header;
        NodeId senderId;
    };

    // Mark the specified path name or use the path names matching the
    // pattern for removal.
    struct NsRemoveMsg {
        NsHeader header;
        bool pathNameIsPattern;
    };

    // Close the specified handle.
    struct NsCloseMsg {
        NsHeader header;
        LibNsTypes::NsHandle nsHandle;
    };

    // Used to manage msg streams.  One of these is allocated for each
    // received chunk of data.
    struct NsStreamChunk {
        NsStreamChunk *next;
        size_t size;
        uint8_t dataChunk;
    };

    // Used to keep the list of chunks as they are received.  Once they
    // are all received they can be combined into a single response.
    struct NsStreamMgmt {
        Mutex chunkLock;
        NsStreamChunk *head;
    };

    // Passed as eph->ephemeral in twoPc.  These are filled in our
    // completion routine.
    struct MsgNsOutput {
        Status status = StatusOk;
        NsDlmOps ops;
        NsOpOutput opOutput;
        NsObject *nsObject = NULL;
        // Used for getPathInfo streaming.
        NsStreamMgmt *streamMgmt = NULL;
    };

    // Hash table to keep track of which NsId(s) have the object open and
    // ensure close uses the same NsId(s).

    struct OpenTableHTEntry {
        IntHashTableHook hook;
        LibNsTypes::NsId getNsId() const { return openNsId; };
        LibNsTypes::NsId openNsId;
        void del();
    };

    static constexpr size_t OpenTableHashTableSlot = 11;

    typedef IntHashTable<LibNsTypes::NsId,
                         OpenTableHTEntry,
                         &OpenTableHTEntry::hook,
                         &OpenTableHTEntry::getNsId,
                         OpenTableHashTableSlot,
                         hashIdentity>
        OpenTableHashTable;

    // This object is used by the LibNs dlm node to manage the object
    // associated with a path name.  This object "encompasses" the client
    // specified object along with "bookkeeping" information needed to
    // manage the object.

    struct NsDlmObject {
        LibNsTypes::NsId nsId;
        char pathName[LibNsTypes::MaxPathNameLen];
        // Used by removeMatchingLocal
        NsDlmObject *next;
        Atomic32 refCount;
        uint32_t version;
        bool openedReaderWriterExcl;
        bool openedReaderShared;
        bool openedReaderSharedWriterExcl;
        uint32_t numReaderOpens;
        uint32_t numWriterOpens;
        Stopwatch startTimer;

        // Hash table of NsIds used to open this object
        OpenTableHashTable *openTable;
        bool pendingRemove;
        LibNsTypes::NsAutoRemove autoRemove;
        // DLM's copy of the object published with the path name
        uint8_t nsObject[];
    };

    // Hash table to provide path name to NsId mapping.

    struct PathNameToNsIdEntry {
        char pathName[LibNsTypes::MaxPathNameLen];
        LibNsTypes::NsId nsId;
        StringHashTableHook hook;
        const char *getPathName() const { return pathName; };
        void del();
    };

    static constexpr size_t PathNameToNsIdHashTableSlot = 4096;
    static constexpr size_t NsIdToNsDlmObjectHashTableSlot = 4096;

    typedef StringHashTable<PathNameToNsIdEntry,
                            &PathNameToNsIdEntry::hook,
                            &PathNameToNsIdEntry::getPathName,
                            PathNameToNsIdHashTableSlot,
                            hashStringFast>
        PathNameToNsIdHashTable;
    PathNameToNsIdHashTable pathNameToNsIdHashTable_;

    // Hash table to provide NsId to NsObject mapping

    struct NsIdToNsDlmObjectEntry {
        IntHashTableHook hook;
        LibNsTypes::NsId getNsId() const { return nsId; };
        LibNsTypes::NsId nsId;
        NsDlmObject *nsDlmObject;
        void del();
    };

    IntHashTable<LibNsTypes::NsId,
                 NsIdToNsDlmObjectEntry,
                 &NsIdToNsDlmObjectEntry::hook,
                 &NsIdToNsDlmObjectEntry::getNsId,
                 NsIdToNsDlmObjectHashTableSlot,
                 hashIdentity>
        nsIdToNsDlmObjectHashTable_;

    Mutex dlmLock_;

    // Stats
    static constexpr size_t StatCount = 8;
    StatGroupId namespaceStatGroupId_;
    struct {
        StatHandle published;
        StatHandle removed;
        StatHandle updated;
        StatHandle lookedup;
        StatHandle opened;
        StatHandle closed;
        StatHandle streamedCnt;
        StatHandle streamedBytes;
    } stats_;

    MustCheck LibNsTypes::NsId publishInternal(
        const char *pathName,
        NsObject *nsObj,
        Status *status,
        LibNsTypes::NsAutoRemove autoRemove,
        NodeId objNodeId);
    MustCheck Status dispatchDlm(NodeId dlmNode,
                                 void *payload,
                                 size_t payloadSize,
                                 void *response);
    MustCheck LibNsTypes::NsHandle openCommon(NsOpenMsg openMsg,
                                              NsObject **nsObject,
                                              Status *status,
                                              NodeId objNodeId);
    MustCheck Status markForRemovalCommon(const char *pathName,
                                          bool *objDeleted,
                                          NodeId objNodeId);
    MustCheck Status markForRemovalCommon(LibNsTypes::NsId nsId,
                                          bool *objDeleted);

    MustCheck Status publishLocal(void *payload,
                                  void **retOutput,
                                  size_t *retOutputSize);

    MustCheck Status openLocal(void *payload,
                               void **retOutput,
                               size_t *retOutputSize);

    MustCheck Status closeLocal(void *payload,
                                void **retOutput,
                                size_t *retOutputSize);

    MustCheck Status getDlmObj(NsHeader *nsHdr, NsDlmObject **dlmObj);

    MustCheck Status getInfoLocal(void *payload,
                                  void **retOutput,
                                  size_t *retOutputSize);

    MustCheck NsObject *getNsObjectDlmLocal(LibNsTypes::NsHandle nsHandle);

    MustCheck Status getPathInfoLocal(void *payload,
                                      void **retOutput,
                                      size_t *retOutputSize,
                                      MsgEphemeral *ephIn);

    MustCheck Status updateLocal(void *payload,
                                 void **retOutput,
                                 size_t *retOutputSize);

    MustCheck Status removeLocal(void *payload,
                                 void **retOutput,
                                 size_t *retOutputSize);

    MustCheck Status removeMatchingLocal(void *payload,
                                         void **retOutput,
                                         size_t *retOutputSize);

    MustCheck Status openHelper(LibNsTypes::NsOpenFlags openFlags,
                                NsDlmObject *dlmObj);

    void closeHelper(LibNsTypes::NsOpenFlags openFlags, NsDlmObject *dlmObj);

    // Called internally to delete dlm Object
    void deleteDlmObject(NsDlmObject *nsDlmObject);

    // Ref counting - didn't use the existing implementation as it
    // requires a static function pointer...a c++ no-no.
    void refInit(Atomic32 *refCount);
    void refInc(Atomic32 *refCount);
    void refDec(Atomic32 *refCount);
    MustCheck uint32_t refRead(const Atomic32 *refCount);

    // Stats
    void statsIncPublished() { StatsLib::statAtomicIncr64(stats_.published); };
    void statsIncRemoved() { StatsLib::statAtomicIncr64(stats_.removed); };
    void statsIncUpdated() { StatsLib::statAtomicIncr64(stats_.updated); };
    void statsIncLookedup() { StatsLib::statAtomicIncr64(stats_.lookedup); };
    void statsIncOpened() { StatsLib::statAtomicIncr64(stats_.opened); };
    void statsIncClosed() { StatsLib::statAtomicIncr64(stats_.closed); };
    void statsIncStreamedCnt()
    {
        StatsLib::statAtomicIncr64(stats_.streamedCnt);
    };
    void statsAddStreamedBytes(size_t numBytes)
    {
        StatsLib::statAtomicAdd64(stats_.streamedBytes, numBytes);
    };

    // msgStream
    MustCheck Status sendViaStream(NodeId destNodeId,
                                   MsgEphemeral *ephIn,
                                   void *pathInfo,
                                   size_t infoSize);

    LibNs() {}   // Use init
    ~LibNs() {}  // Use destroy

    // Disallow
    LibNs(const LibNs &) = delete;
    LibNs &operator=(const LibNs &) = delete;
};

class TwoPcMsg2pcDlmUpdateNs1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcDlmUpdateNs1() {}
    virtual ~TwoPcMsg2pcDlmUpdateNs1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcDlmUpdateNs1(const TwoPcMsg2pcDlmUpdateNs1 &) = delete;
    TwoPcMsg2pcDlmUpdateNs1(const TwoPcMsg2pcDlmUpdateNs1 &&) = delete;
    TwoPcMsg2pcDlmUpdateNs1 &operator=(const TwoPcMsg2pcDlmUpdateNs1 &) =
        delete;
    TwoPcMsg2pcDlmUpdateNs1 &operator=(const TwoPcMsg2pcDlmUpdateNs1 &&) =
        delete;
};

#endif  // _LIBNS_H_
