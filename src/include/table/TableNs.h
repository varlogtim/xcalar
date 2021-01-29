// Copyright 2013 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _TABLE_NS_H_
#define _TABLE_NS_H_

#include "primitives/Primitives.h"
#include "operators/GenericTypes.h"
#include "libapis/LibApisCommon.h"
#include "udf/UserDefinedFunction.h"
#include "dag/DagTypes.h"

class TableIdRecord;

// Class to support Name Space mechanism associated with Table
// Provides propagation of Table information across nodes
// Provides control for access (lock) to Table object through open/close
// interface
// Provides functionality for TableService interface.
class TableNsMgr final
{
  public:
    typedef Xid TableId;
    static constexpr const VersionId InvalidVersion = InvalidVersion;
    static constexpr const Xid InvalidTableId = XidInvalid;
    static constexpr const VersionId StartVersion = 1;

    // XXX Todo may need to parameterize this
    static constexpr const uint64_t OpenNoRetry = 0;
    static constexpr const uint64_t OpenNoSleep = 0;
    static constexpr const uint64_t OpenMaxRetries = 1000;
    static constexpr const uint64_t OpenSleepInUsecs = USecsPerMSec * 10;

    // Extension for Name space handle
    struct IdHandle {
        TableId tableId;
        XcalarApiUdfContainer sessionContainer;
        LibNsTypes::NsHandle nsHandle;
        VersionId consistentVersion = InvalidVersion;
        VersionId nextVersion = InvalidVersion;
        bool isGlobal = false;
        bool mergePending = false;
    };

    // Wrapper for IdHandle
    struct TableHandleTrack {
        TableId tableId = TableNsMgr::InvalidTableId;
        IdHandle tableHandle;
        bool tableHandleValid = false;
        TableHandleTrack() {}
    };

    static MustCheck TableNsMgr *get();
    static MustCheck Status init();
    void destroy();

    // Generates new and unique Table ID
    MustCheck TableId genTableId();

    MustCheck bool isTableIdValid(TableId tableId) const
    {
        return tableId != InvalidTableId;
    }

    // Returns table names which are published
    // Caller is resposible to delete retTableNames and its entries using
    // 'delete' operator.
    // Returns Status OK if pattern does not match, retNumTables set to 0 and
    // retTableNames is set nullptr
    MustCheck Status listGlobalTables(
        const char *pattern,  // in:  Regular expression pattern to match table
                              //      names
        char **&retTableNames,// out: List of selected Table names
        size_t &retNumTables);// out: Number of entries in retTableNames

    // Returns table names which are associated with specified session
    // Caller is resposible to delete retTableNames and its entries using
    // 'delete' operator.
    // Returns Status OK if pattern does not match, retNumTables set to 0 and
    // retTableNames is set nullptr
    MustCheck Status
    listSessionTables(
        const char *pattern,  // in:  Regular expression pattern to match table
                              //      names
        char **&retTableNames,// out: List of selected Table names
        size_t &retNumTables, // out: Number of entries in retTableNames
        const XcalarApiUdfContainer *sessionContainer); // in: Session which
                                                        //     contains tables

    // Check if Table specified by fully qualified name is published
    // retStatus returns retStatus is the Table is not found
    // Note: used in TableMgr::getDagNodeInfo only
    MustCheck bool isGlobal(
        const char *fullyQualName, // in:  Fully qualified name to specify Table
        Status &retStatus);        // out: Result status

    // Publish (mark as global) Table specified by Name (not qualified) for
    // the session
    inline MustCheck Status publishTable(
        const XcalarApiUdfContainer *sessionContainer, // Session which contains
                                                       // the table
        const char *tableName) // Plane (not qialified) table name
    {
        return publishOrUnpublishTable(sessionContainer, tableName, true);
    }

    // Unpublish (mark as local) Table specified by Name (not qualified) for
    // the session
    inline MustCheck Status unpublishTable(
        const XcalarApiUdfContainer *sessionContainer, // User session which
                                                       // contains the table
        const char *tableName) // Plane (not qialified) table name
    {
        return publishOrUnpublishTable(sessionContainer, tableName, false);
    }

    // Create and add table to the name space associated with the session.
    MustCheck Status addToNs(
        const XcalarApiUdfContainer *sessionContainer, // in: Session container
        TableId tableId,            // in: table Id created by genTableId
        const char *tableName,      // in: Plane (not qualified) table name
        DagTypes::DagId dagId,      // in: dag, associated with the Table
        DagTypes::NodeId dagNodeId);// in: dag node, associated with the Table

    // Remove table from name space and from session. Deletion is propagated
    // throuh all Xcalar nodes
    void removeFromNs(const XcalarApiUdfContainer *sessionContainer,
                      TableId tableId,
                      const char *tableName);

    // Recreate table name as copy of old object. Object with old name is
    // destroyed. All associations are preserved
    // Note: Not independent, must be called from Dag::renameDagNode only
    MustCheck Status renameNs(const XcalarApiUdfContainer *sessionContainer,
                              TableId tableId,
                              const char *tableNameOld,
                              const char *tableNameNew);

    // Open handle to specified table. The handle may be opend as readonly or
    // exclusive. If object is locked by other operation, the method does not
    // return till the object is available again.
    // XXX: method has no timeout: posible deadlock. Need to be fixed.
    MustCheck Status
    openHandleToNs(
        const XcalarApiUdfContainer *sessionContainer, // in: User session where
                                                       //     table exists
        TableId tableId, // in:
        LibNsTypes::NsOpenFlags openFlag, // in: Open flags: object locking type
        IdHandle *retHandle, // out: pointer to handle structure to fill.
        uint64_t sleepInUsecs); // in: Sleep period between locking attempts.

    // Update NS Object instance. Old object is destroyed and new one replaces
    // it. Reinitialize and return new handle object.
    MustCheck IdHandle updateNsObject(IdHandle *handle, Status &retStatus);

    // Close handle and release NS object. Release object lock.
    void closeHandleToNs(IdHandle *handleIn);

    // This part is resposible for processing Fully Qualified name
    // Verify if name is Fully Qualified name.
    MustCheck inline static bool isValidFullyQualTableName(
        const char *const tableName)
    {
        return (strncmp(tableName, TableNamesNsPrefix, TableNamesNsPrefixLen) ==
                0);
    }

    // Exctracts Table name from Fully Qualified name.
    // Caller is resposible to delete pointer returned in retTableName using
    // delete operator.
    MustCheck static Status getNameFromFqTableName(const char *fullyQualName,
                                            char *&retTableName);

    // Form Fully qualified name for Table in specified session
    MustCheck static Status getFQN(
        char *retFqn, // out: buffer where fully qualified name will be stored
        size_t fqnLen, // in: size of the buffer
        const XcalarApiUdfContainer *sessionContainer, // User session
        const char *tableName); // Plane Table name

  private:
    static constexpr const char *ModuleName = "TableNs";
    static TableNsMgr *instance;

    // Table names NS. "/tableName/<UserName>/<SessionId>/<tableName>"
    static constexpr const char *TableNamesNsPrefix = "/tableName";
    static const size_t TableNamesNsPrefixLen = strlen(TableNamesNsPrefix);

    // Table ID NS. "/tableId/<tableId>"
    static constexpr const char *TableIdsNsPrefix = "/tableId";
    static const size_t TableIdsNsPrefixLen = strlen(TableIdsNsPrefix);

    // Given Legacy retinas are still hanging out in the code, need to
    // capture these in the NS differently, since these tables don't have
    // Session scope.
    static constexpr const char *LegacyRetina = "LegacyRetina";

    MustCheck Status initInternal();

    MustCheck Status getFQN(char *retFqn, size_t fqnLen, TableId tableId) const;

    MustCheck Status
    addToNsInternal(const XcalarApiUdfContainer *sessionContainer,
                    TableId tableId,
                    const char *tableName,
                    const char *fullyQualName,
                    bool tableNameOnly);

    void removeFromNsInternal(const XcalarApiUdfContainer *sessionContainer,
                              TableId tableId,
                              const char *tableName,
                              bool tableNameOnly);

    MustCheck Status
    publishOrUnpublishTable(const XcalarApiUdfContainer *sessionContainer,
                            const char *tableName,
                            bool isGlobal);

    MustCheck Status
    getTableIdFromName(const XcalarApiUdfContainer *sessionContainer,
                       const char *tableName,
                       TableId *retTableId) const;

    MustCheck Status getIdRecord(const char *fullyQualifiedId,
                                 TableIdRecord **retIdRecord) const;

    MustCheck Status getIdRecord(TableId tableId,
                                 TableIdRecord **retIdRecord) const;

    MustCheck Status
    openHandleToNsWithName(const XcalarApiUdfContainer *sessionContainer,
                           const char *tableName,
                           LibNsTypes::NsOpenFlags openFlag,
                           IdHandle *retHandle,
                           uint64_t sleepInUsecs);

    TableNsMgr() {}
    ~TableNsMgr() {}
    TableNsMgr(const TableNsMgr &) = delete;
    TableNsMgr &operator=(const TableNsMgr &) = delete;
};

#endif  // _TABLE_NS_H_
