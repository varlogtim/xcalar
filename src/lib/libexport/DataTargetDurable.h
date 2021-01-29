// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

// Updating durable data structures must be done with care.  Please carefully
// read http://wiki.int.xcalar.com/mediawiki/index.php/Durables for instructions.

// This file AUTOGENERATED
// DO NOT EDIT THIS FILE BY HAND

// ===> http://wiki.int.xcalar.com/mediawiki/index.php/Durables for details

#ifndef _DATATARGETDURABLE_H_
#define _DATATARGETDURABLE_H_

#include <exception>
#include "StrlFunc.h"
#include "sys/XLog.h"

#include "export/DataTarget.h"
#include "subsys/DurableDataTarget.pb.h"

class DataTargetDurable{
public:
    static MustCheck Status pbSerTargetListPersistedDataPub(DataTargetManager::TargetListPersistedData *raw, xcalar::internal::durable::datatarget::TargetListPersistedData *pb) {
        try {
            pbSerTargetListPersistedData(raw, pb);
            return StatusOk;
        } catch(std::bad_alloc) {
            return(StatusNoMem);
        } catch(std::invalid_argument) {
            return(StatusInval);
        } catch(std::exception& e) {
            return(StatusProtobufEncodeError);
        }
    }

    static MustCheck Status pbDesTargetListPersistedDataPub(void **node, DataTargetManager::TargetListPersistedData *raw, xcalar::internal::durable::datatarget::TargetListPersistedData *pb, const char * const idlSha) {
        try {
            pbDesTargetListPersistedData(node, raw, pb, idlSha);
            return StatusOk;
        } catch(std::bad_alloc) {
            return(StatusNoMem);
        } catch(std::invalid_argument) {
            return(StatusInval);
        } catch(std::exception& e) {
            return(StatusProtobufDecodeError);
        }
    }

    // ------------------ Union Size Routines -----------------
    static size_t pbUnionSizeExAddTargetSpecificInput(const int unionType);
private:
    static constexpr const char *ModuleName = "libDataTargetDurable";
    DataTargetDurable() {}
    ~DataTargetDurable() {}
    DataTargetDurable(const DataTargetDurable&) = delete;
    DataTargetDurable(const DataTargetDurable&&) = delete;
    DataTargetDurable& operator=(const DataTargetDurable&) = delete;
    DataTargetDurable& operator=(const DataTargetDurable&&) = delete;
    
    // ---------------- Serialization Routines ----------------
    static void pbSerExAddTargetSFInput(ExAddTargetSFInput *raw, xcalar::internal::durable::datatarget::ExAddTargetSFInput *pb);
    static void pbSerExAddTargetSpecificInput(ExAddTargetSpecificInput *raw, xcalar::internal::durable::datatarget::ExAddTargetSpecificInput *pb, const int unionType);
    static void pbSerExAddTargetUDFInput(ExAddTargetUDFInput *raw, xcalar::internal::durable::datatarget::ExAddTargetUDFInput *pb);
    static void pbSerExExportTarget(ExExportTarget *raw, xcalar::internal::durable::datatarget::ExExportTarget *pb);
    static void pbSerExExportTargetHdr(ExExportTargetHdr *raw, xcalar::internal::durable::datatarget::ExExportTargetHdr *pb);
    static void pbSerTargetListPersistedData(DataTargetManager::TargetListPersistedData *raw, xcalar::internal::durable::datatarget::TargetListPersistedData *pb);
    static void pbSerTargetPersistHeader(DataTargetManager::TargetPersistHeader *raw, xcalar::internal::durable::datatarget::TargetPersistHeader *pb);
    static void pbSertimespec(timespec *raw, xcalar::internal::durable::datatarget::timespec *pb);
    
    // --------------- Deserialization Routines ---------------
    static void pbDesExAddTargetSFInput(void **node, ExAddTargetSFInput *raw, xcalar::internal::durable::datatarget::ExAddTargetSFInput *pb, const char * const idlSha);
    static void pbDesExAddTargetSpecificInput(void **node, ExAddTargetSpecificInput *raw, xcalar::internal::durable::datatarget::ExAddTargetSpecificInput *pb, const char * const idlSha, const int unionType);
    static void pbDesExAddTargetUDFInput(void **node, ExAddTargetUDFInput *raw, xcalar::internal::durable::datatarget::ExAddTargetUDFInput *pb, const char * const idlSha);
    static void pbDesExExportTarget(void **node, ExExportTarget *raw, xcalar::internal::durable::datatarget::ExExportTarget *pb, const char * const idlSha);
    static void pbDesExExportTargetHdr(void **node, ExExportTargetHdr *raw, xcalar::internal::durable::datatarget::ExExportTargetHdr *pb, const char * const idlSha);
    static void pbDesTargetListPersistedData(void **node, DataTargetManager::TargetListPersistedData *raw, xcalar::internal::durable::datatarget::TargetListPersistedData *pb, const char * const idlSha);
    static void pbDesTargetPersistHeader(void **node, DataTargetManager::TargetPersistHeader *raw, xcalar::internal::durable::datatarget::TargetPersistHeader *pb, const char * const idlSha);
    static void pbDestimespec(void **node, timespec *raw, xcalar::internal::durable::datatarget::timespec *pb, const char * const idlSha);
    
    // --------------- Minor Versioning Routines --------------
    static void pbDesExAddTargetSFInputMinorVers(void **node, ExAddTargetSFInput **raw, xcalar::internal::durable::datatarget::ExAddTargetSFInput *pb, const char * const idlSha);
    static void pbDesExAddTargetSpecificInputMinorVers(void **node, ExAddTargetSpecificInput **raw, xcalar::internal::durable::datatarget::ExAddTargetSpecificInput *pb, const char * const idlSha, const int unionType);
    static void pbDesExAddTargetUDFInputMinorVers(void **node, ExAddTargetUDFInput **raw, xcalar::internal::durable::datatarget::ExAddTargetUDFInput *pb, const char * const idlSha);
    static void pbDesExExportTargetMinorVers(void **node, ExExportTarget **raw, xcalar::internal::durable::datatarget::ExExportTarget *pb, const char * const idlSha);
    static void pbDesExExportTargetHdrMinorVers(void **node, ExExportTargetHdr **raw, xcalar::internal::durable::datatarget::ExExportTargetHdr *pb, const char * const idlSha);
    static void pbDesTargetListPersistedDataMinorVers(void **node, DataTargetManager::TargetListPersistedData **raw, xcalar::internal::durable::datatarget::TargetListPersistedData *pb, const char * const idlSha);
    static void pbDesTargetPersistHeaderMinorVers(void **node, DataTargetManager::TargetPersistHeader **raw, xcalar::internal::durable::datatarget::TargetPersistHeader *pb, const char * const idlSha);
    static void pbDestimespecMinorVers(void **node, timespec **raw, xcalar::internal::durable::datatarget::timespec *pb, const char * const idlSha);
    // ------------- Minor Versioning PreRoutines -------------
    static void pbDesExAddTargetSFInputMinorVersPre(void **node, ExAddTargetSFInput **raw, xcalar::internal::durable::datatarget::ExAddTargetSFInput *pb, const char * const idlSha);
    static void pbDesExAddTargetSpecificInputMinorVersPre(void **node, ExAddTargetSpecificInput **raw, xcalar::internal::durable::datatarget::ExAddTargetSpecificInput *pb, const char * const idlSha, const int unionType);
    static void pbDesExAddTargetUDFInputMinorVersPre(void **node, ExAddTargetUDFInput **raw, xcalar::internal::durable::datatarget::ExAddTargetUDFInput *pb, const char * const idlSha);
    static void pbDesExExportTargetMinorVersPre(void **node, ExExportTarget **raw, xcalar::internal::durable::datatarget::ExExportTarget *pb, const char * const idlSha);
    static void pbDesExExportTargetHdrMinorVersPre(void **node, ExExportTargetHdr **raw, xcalar::internal::durable::datatarget::ExExportTargetHdr *pb, const char * const idlSha);
    static void pbDesTargetListPersistedDataMinorVersPre(void **node, DataTargetManager::TargetListPersistedData **raw, xcalar::internal::durable::datatarget::TargetListPersistedData *pb, const char * const idlSha);
    static void pbDesTargetPersistHeaderMinorVersPre(void **node, DataTargetManager::TargetPersistHeader **raw, xcalar::internal::durable::datatarget::TargetPersistHeader *pb, const char * const idlSha);
    static void pbDestimespecMinorVersPre(void **node, timespec **raw, xcalar::internal::durable::datatarget::timespec *pb, const char * const idlSha);};
#endif