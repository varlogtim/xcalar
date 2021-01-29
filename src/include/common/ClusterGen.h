// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _CLUSTER_GEN_H_
#define _CLUSTER_GEN_H_

const char *getClusterUuidStr();
uint64_t getClusterGeneration();
Status addClusterGen();
Status lookupClusterGen();
Status updateClusterGen();

#endif  // _CLUSTER_GEN_H_
