// Copyright 2018 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _SERVICEREGISTRY_H_
#define _SERVICEREGISTRY_H_

#include "EchoService.h"
#include "KvStoreService.h"
#include "PublishedTableService.h"
#include "UserDefinedFunctionService.h"
#include "CgroupService.h"
#include "QueryService.h"
#include "TableService.h"
#include "LicenseService.h"
#include "StatsService.h"
#include "ResultSetService.h"
#include "DataflowService.h"
#include "DagNodeService.h"
#include "ConnectorsService.h"
#include "OperatorService.h"
#include "SchemaLoadService.h"
#include "AppService.h"
#include "VersionService.h"
#include "LogService.h"
#include "MemoryService.h"

template <typename T>
IService *
constructService()
{
    return new (std::nothrow) T();
}

typedef IService *(*ServiceBuilder)();

//
// Here we provide the user-written (not generated) implementations for all of
// our services. Your service must be on this list in order for an instance of
// the service to be created at startup time. Otherwise, the service will not be
// made available at runtime.
//
ServiceBuilder serviceBuilders[] =
    {constructService<xcalar::compute::localtypes::Echo::EchoService>,
     constructService<xcalar::compute::localtypes::KvStore::KvStoreService>,
     constructService<xcalar::compute::localtypes::log::LogService>,
     constructService<xcalar::compute::localtypes::memory::MemoryService>,
     constructService<
         xcalar::compute::localtypes::PublishedTable::PublishedTableService>,
     constructService<
         xcalar::compute::localtypes::UDF::UserDefinedFunctionService>,
     constructService<xcalar::compute::localtypes::Cgroup::CgroupService>,
     constructService<xcalar::compute::localtypes::Query::QueryService>,
     constructService<xcalar::compute::localtypes::Table::TableService>,
     constructService<xcalar::compute::localtypes::License::LicenseService>,
     constructService<xcalar::compute::localtypes::Stats::StatsService>,
     constructService<xcalar::compute::localtypes::ResultSet::ResultSetService>,
     constructService<xcalar::compute::localtypes::Dataflow::DataflowService>,
     constructService<xcalar::compute::localtypes::DagNode::DagNodeService>,
     constructService<
         xcalar::compute::localtypes::Connectors::ConnectorsService>,
     constructService<
         xcalar::compute::localtypes::SchemaLoad::SchemaLoadService>,
     constructService<xcalar::compute::localtypes::Operator::OperatorService>,
     constructService<xcalar::compute::localtypes::App::AppService>,
     constructService<xcalar::compute::localtypes::Version::VersionService>};

#endif  // _SERVICEREGISTRY_H_
