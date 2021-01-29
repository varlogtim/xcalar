// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef APPFUNCTESTS_H
#define APPFUNCTESTS_H

#include "primitives/Primitives.h"
#include "test/FuncTests/FuncTestBase.h"
#include "util/RandomTypes.h"
#include "config/Config.h"

class TestApp
{
  public:
    TestApp(const char *name,
            App::HostType hostType,
            uint32_t flags,
            const char *source)
        : name_(name), hostType_(hostType), flags_(flags), source_(source)
    {
    }

    virtual ~TestApp() {}

    static char *flattenOutput(const char *outBlob);
    virtual Status execute() = 0;

    const char *name_;
    App::HostType hostType_;
    uint32_t flags_;
    const char *source_;
};

class BigOutput : public TestApp
{
  public:
    static constexpr const char *Source =
        "def main(inBlob):                                                  \n"
        "    return 'A' * 150                                              \n";

    BigOutput()
        : TestApp("bigOutput",
                  App::HostType::Python2,
                  App::FlagInstancePerNode,
                  Source)
    {
    }
    virtual Status execute();
};

class Module : public TestApp
{
  public:
    static constexpr const char *Source =
        "import xcalar.container.context as ctx                          \n"
        "def main(inBlob):                                                   \n"
        "    thisXpuId = ctx.get_xpu_id()                                      "
        "\n"
        "    thisNodeId = ctx.getNodeId(thisXpuId)                           \n"
        "    return str(thisNodeId)                                         \n";

    Module()
        : TestApp("module",
                  App::HostType::Python2,
                  App::FlagInstancePerNode,
                  Source)
    {
    }
    virtual Status execute();
};

class Errors : public TestApp
{
  public:
    static constexpr const char *Source =
        "def main(inBlob):                                                   \n"
        "    return str(4 // 0)                                             \n";

    Errors()
        : TestApp("errors",
                  App::HostType::Python2,
                  App::FlagInstancePerNode,
                  Source)
    {
    }
    virtual Status execute();
};

class Pause : public TestApp
{
  public:
    static constexpr const char *Source =
        "import time\ndef main(inBlob): time.sleep(1); return None";

    Pause()
        : TestApp("pause",
                  App::HostType::Python2,
                  App::FlagInstancePerNode,
                  Source)
    {
    }
    virtual Status execute();
};

class Echo : public TestApp
{
  public:
    static constexpr const char *Source =
        "def main(inBlob):                                                   \n"
        "    return inBlob                                                  \n";

    Echo()
        : TestApp("echo",
                  App::HostType::Python2,
                  App::FlagInstancePerNode,
                  Source)
    {
    }
    virtual Status execute();
};

class BarrierSi : public TestApp
{
  public:
    static constexpr const char *Source =
        "# single instance: all instances wait on barrier and return 'A'     \n"
        "# validation will ensure all instances have returned 'A'            \n"
        "# main pass criteria is no hang and all returned 'A'                \n"
        "import xcalar.container.xpuComm as xpuComm                      \n"
        "def main(inBlob):                                                   \n"
        "    xpuComm.xpuBarrierWait()                                        \n"
        "    return 'A'                                                     \n";

    BarrierSi()
        : TestApp("barrierSi",
                  App::HostType::Python2,
                  App::FlagInstancePerNode,
                  Source)
    {
    }
    virtual Status execute();
};

class BarrierStressSi : public TestApp
{
  public:
    static constexpr const char *Source =
        "# single instance: all instances issue a fixed number of            \n"
        "# back-to-back calls to barrier to stress the synchronization logic \n"
        "# of send/recv between consecutive barrier calls. Pass criteria is  \n"
        "# no hang and all eventually returned 'A'                           \n"
        "import xcalar.container.xpuComm as xpuComm                      \n"
        "import xcalar.container.xpu_host as xpu_host                        \n"
        "def main(inBlob):                                                   \n"
        "    thisXpuId = xpu_host.get_xpu_id()                             \n"
        "    for i in range(11):                                             \n"
        "        xpuComm.xpuBarrierWait()                                    \n"
        "    return 'A'                                                     \n";

    BarrierStressSi()
        : TestApp("barrierStressSi",
                  App::HostType::Python2,
                  App::FlagInstancePerNode,
                  Source)
    {
    }
    virtual Status execute();
};

class BarrierStressMi : public TestApp
{
  public:
    static constexpr const char *Source =
        "# multi instance: all instances issue a fixed number of             \n"
        "# back-to-back calls to barrier to stress the synchronization logic \n"
        "# of send/recv between consecutive barrier calls. Pass criteria is  \n"
        "# no hang and all eventually returned 'A'                           \n"
        "import xcalar.container.xpuComm as xpuComm                      \n"
        "import xcalar.container.xpu_host as xpu_host                        \n"
        "def main(inBlob):                                                   \n"
        "    thisXpuId = xpu_host.get_xpu_id()                             \n"
        "    for i in range(11):                                             \n"
        "        xpuComm.xpuBarrierWait()                                    \n"
        "    return 'A'                                                     \n";

    BarrierStressMi()
        : TestApp("barrierStressMi",
                  App::HostType::Python2,
                  App::FlagNone,
                  Source)
    {
    }
    virtual Status execute();
};

class BarrierSendRecvSi : public TestApp
{
  public:
    static constexpr const char *Source =
        "# single instance: all instances issue a fixed number of            \n"
        "# back-to-back calls to barrier and then send/recv to stress the    \n"
        "# synchronization logic of send/recv to XPUs while they may still   \n"
        "# be in the barrier. Pass criteria is no hang and all eventually    \n"
        "# returned 'A'                                                      \n"
        "import xcalar.container.xpuComm as xpuComm                      \n"
        "import xcalar.container.xpu_host as xpu_host                        \n"
        "def main(inBlob):                                                   \n"
        "    thisXpuId = xpu_host.get_xpu_id()                             \n"
        "    xpuClusSz = xpu_host.get_xpu_cluster_size()                    \n"
        "    if xpuClusSz == 1:                                              \n"
        "        return 'A'                                                  \n"
        "    for i in range(11):                                             \n"
        "        xpuComm.xpuBarrierWait()                                    \n"
        "        if thisXpuId == 0:                                          \n"
        "            for j in range(xpuClusSz - 1):                          \n"
        "                xpuComm.xpuSend(j+1,                                \n"
        "                     'hello world from xpu 0'.encode('utf-8'))      \n"
        "        else:                                                       \n"
        "            msg = xpuComm.xpuRecv()                                 \n"
        "    return 'A'                                                     \n";

    BarrierSendRecvSi()
        : TestApp("barrierSendRecvSi",
                  App::HostType::Python2,
                  App::FlagInstancePerNode,
                  Source)
    {
    }
    virtual Status execute();
};

class BarrierSendRecvMi : public TestApp
{
  public:
    static constexpr const char *Source =
        "# multi instance: all instances issue a fixed number of             \n"
        "# back-to-back calls to barrier and then send/recv to stress the    \n"
        "# synchronization logic of send/recv to XPUs while they may still   \n"
        "# be in the barrier. Pass criteria is no hang and all eventually    \n"
        "# returned 'A'                                                      \n"
        "import xcalar.container.xpuComm as xpuComm                      \n"
        "import xcalar.container.xpu_host as xpu_host                        \n"
        "def main(inBlob):                                                   \n"
        "    thisXpuId = xpu_host.get_xpu_id()                             \n"
        "    xpuClusSz = xpu_host.get_xpu_cluster_size()                    \n"
        "    if xpuClusSz == 1:                                              \n"
        "        return 'A'                                                  \n"
        "    for i in range(11):                                             \n"
        "        xpuComm.xpuBarrierWait()                                    \n"
        "        if thisXpuId == 0:                                          \n"
        "            for j in range(xpuClusSz - 1):                          \n"
        "                xpuComm.xpuSend(j+1,                                \n"
        "                     'hello world from xpu 0'.encode('utf-8'))      \n"
        "        else:                                                       \n"
        "            msg = xpuComm.xpuRecv()                                 \n"
        "    return 'A'                                                     \n";

    BarrierSendRecvMi()
        : TestApp("barrierSendRecvMi",
                  App::HostType::Python2,
                  App::FlagNone,
                  Source)
    {
    }
    virtual Status execute();
};

class BarrierMi : public TestApp
{
  public:
    static constexpr const char *Source =
        "# multi instance: all instances wait on barrier and return 'B'      \n"
        "# validation will ensure all instances have returned 'B'            \n"
        "# main pass criteria is no hang and all returned 'B'                \n"
        "import xcalar.container.xpuComm as xpuComm                      \n"
        "def main(inBlob):                                                   \n"
        "    xpuComm.xpuBarrierWait()                                        \n"
        "    return 'B'                                                     \n";

    BarrierMi()
        : TestApp("barrierMi", App::HostType::Python2, App::FlagNone, Source)
    {
    }
    virtual Status execute();
};

class BarrierSiRand : public TestApp
{
  public:
    static constexpr const char *Source =
        "# Single instance: all instances sleep for random times, wait on a  \n"
        "# barrier and finally return 'B' after exiting the barrier.         \n"
        "# Validation will ensure all instances have returned 'A'            \n"
        "# main pass criteria is no hang and all returned 'A'                \n"
        "import xcalar.container.xpuComm as xpuComm                      \n"
        "from random import randint                                          \n"
        "from time import sleep                                              \n"
        "def main(inBlob):                                                   \n"
        "    sleep(randint(50,1000) // 1000.0) # sleep random 50ms->1sec     \n"
        "    xpuComm.xpuBarrierWait()                                        \n"
        "    return 'A'                                                     \n";
    BarrierSiRand()
        : TestApp("barrierSiRand",
                  App::HostType::Python2,
                  App::FlagInstancePerNode,
                  Source)
    {
    }
    virtual Status execute();
};

class BarrierMiRand : public TestApp
{
  public:
    static constexpr const char *Source =
        "# Multi instance: all instances sleep for random times, wait on a   \n"
        "# barrier and finally return 'B' after exiting the barrier.         \n"
        "# Validation will ensure all instances have returned 'B'            \n"
        "# main pass criteria is no hang and all returned 'B'                \n"
        "import xcalar.container.xpuComm as xpuComm                      \n"
        "from random import randint                                          \n"
        "from time import sleep                                              \n"
        "def main(inBlob):                                                   \n"
        "    sleep(randint(50,1000) // 1000.0)                               \n"
        "    xpuComm.xpuBarrierWait()                                        \n"
        "    return 'B'                                                     \n";

    BarrierMiRand()
        : TestApp("barrierMiRand",
                  App::HostType::Python2,
                  App::FlagNone,
                  Source)
    {
    }
    virtual Status execute();
};

class BarrierMiFailure : public TestApp
{
  public:
    static constexpr const char *Source =
        "# Multi instance: all instances sleep for random times, but one     \n"
        "# fails either by raising an exception or sending itself a kill -9. \n"
        "# This should trigger an app abort i.e. there should be no hang. The\n"
        "# return of 'B' isn't checked since the expectation is for the app  \n"
        "# fail / abort itself, not to return 'B'.                           \n"
        "import xcalar.container.xpuComm as xpuComm                      \n"
        "import xcalar.container.xpu_host as xpu_host                        \n"
        "import os                                                           \n"
        "from random import randint                                          \n"
        "from time import sleep                                              \n"
        "def main(inBlob):                                                   \n"
        "    thisXpuId = xpu_host.get_xpu_id()                             \n"
        "    xpuClusSz = xpu_host.get_xpu_cluster_size()                    \n"
        "    failingXpu = (xpuClusSz-1) // 2                                 \n"
        "    if thisXpuId == failingXpu:                                     \n"
        "        suicideType = randint(1,2)                                  \n"
        "        if suicideType == 1:                                        \n"
        "            raise ValueError('barrierMiFailure explicit failure')   \n"
        "        else:                                                       \n"
        "            os.system('kill -9 %d'%(os.getpid()))                   \n"
        "    else:                                                           \n"
        "        sleep(randint(50,1000) // 1000.0)                           \n"
        "    xpuComm.xpuBarrierWait()                                        \n"
        "    return 'B'                                                     \n";

    BarrierMiFailure()
        : TestApp("barrierMiFailure",
                  App::HostType::Python2,
                  App::FlagNone,
                  Source)
    {
    }
    virtual Status execute();
};

class StartFailure : public TestApp
{
  public:
    static constexpr const char *Source = "import notamodule";

    StartFailure()
        : TestApp("startFailure",
                  App::HostType::Python2,
                  App::FlagInstancePerNode,
                  Source)
    {
    }
    virtual Status execute();
};

class XpuSendRecvSiHello : public TestApp
{
  public:
    static constexpr const char *Source =
        "# Single Instance App with XPU 0 sending tiny payload to all XPUs   \n"
        "# Since it's SingleInstance, receivers will be different XCE Nodes  \n"
        "import xcalar.container.xpuComm as xpuComm                      \n"
        "import xcalar.container.xpu_host as xpu_host                        \n"
        "def main(inBlob):                                                   \n"
        "    thisXpuId = xpu_host.get_xpu_id()                             \n"
        "    xpuClusSz = xpu_host.get_xpu_cluster_size()                    \n"
        "    if xpuClusSz == 1:                                              \n"
        "        return 'not engaging' + '\\n'                               \n"
        "    if thisXpuId == 0:                                              \n"
        "        for i in range(xpuClusSz - 1):                              \n"
        "            xpuComm.xpuSend(i+1,                                    \n"
        "                     'hello world from xpu 0'.encode('utf-8'))      \n"
        "        return 'sent messages' + '\\n'                              \n"
        "    else:                                                           \n"
        "        msg = xpuComm.xpuRecv()                                     \n"
        "        return 'received 1 message' + '\\n'                        \n";

    XpuSendRecvSiHello()
        : TestApp("xpuSendRecvSiHello",
                  App::HostType::Python2,
                  App::FlagInstancePerNode,
                  Source)
    {
    }
    virtual Status execute();
};

class XpuSendRecvMiHello : public TestApp
{
  public:
    static constexpr const char *Source =
        "# Multi instance App with XPU 0 sending tiny payload to XPU 1       \n"
        "# Since it's MultiInstance, a XCE node will have numCores XPUs      \n"
        "# which will typically = 4/8 XPUs- so XPU 1 will be on same XCE Node\n"
        "# One more transmission occurs with roles reversed                  \n"
        "import xcalar.container.xpuComm as xpuComm                      \n"
        "import xcalar.container.xpu_host as xpu_host                        \n"
        "def main(inBlob):                                                   \n"
        "    thisXpuId = xpu_host.get_xpu_id()                             \n"
        "    xpuClusSz = xpu_host.get_xpu_cluster_size()                    \n"
        "    if xpuClusSz == 1:                                              \n"
        "        return 'not engaging' + '\\n'                               \n"
        "    if thisXpuId == 0:                                              \n"
        "        xpuComm.xpuSend(1, 'hello world from xpu 0'.encode('utf-8'))\n"
        "        msg = xpuComm.xpuRecv()                                     \n"
        "        return 'sent and received 1 message' + '\\n'                \n"
        "    elif thisXpuId == 1:                                            \n"
        "        msg = xpuComm.xpuRecv()                                     \n"
        "        xpuComm.xpuSend(0, 'hello world from xpu 1'.encode('utf-8'))\n"
        "        return 'received and sent 1 message' + '\\n'                \n"
        "    else:                                                           \n"
        "        return 'not engaging' + '\\n'                              \n";

    XpuSendRecvMiHello()
        : TestApp("xpuSendRecvMiHello",
                  App::HostType::Python2,
                  App::FlagNone,
                  Source)
    {
    }
    virtual Status execute();
};

class XpuSendRecvMiXnodeHello : public TestApp
{
  public:
    static constexpr const char *Source =
        "# Multi instance App: XPU 0 sends tiny payload to XPU on diff node  \n"
        "# Since it's MultiInstance, a XCE node will have numCores XPUs      \n"
        "# and XPU cluster size = nodeCount * xpusPerNode, so XPU on a diff  \n"
        "# node will be at least XPU 0 + xpusPerNode away                    \n"
        "# One more transmission is done with roles reversed                 \n"
        "import xcalar.container.xpuComm as xpuComm                      \n"
        "import xcalar.container.xpu_host as xpu_host                        \n"
        "import xcalar.container.context as xpu                          \n"
        "def main(inBlob):                                                   \n"
        "    thisXpuId = xpu_host.get_xpu_id()                             \n"
        "    xpuClusSz = xpu_host.get_xpu_cluster_size()                    \n"
        "    if xpuClusSz == 1:                                              \n"
        "        return 'not engaging' + '\\n'                               \n"
        "    destXpuId = xpuClusSz - 1                                       \n"
        "    if thisXpuId == 0:                                              \n"
        "        xpuComm.xpuSend(destXpuId,                                  \n"
        "                        'hello world from xpu 0'.encode('utf-8'))   \n"
        "        msg = xpuComm.xpuRecv()                                     \n"
        "        return 'sent and received 1 message' + '\\n'                \n"
        "    elif thisXpuId == destXpuId:                                    \n"
        "        msg = xpuComm.xpuRecv()                                     \n"
        "        msgStr = 'hello world from xpu ' + str(thisXpuId)           \n"
        "        xpuComm.xpuSend(0, msgStr.encode('utf-8'))                  \n"
        "        return 'received and then sent a message' + '\\n'           \n"
        "    else:                                                           \n"
        "        return 'not engaging' + '\\n'                              \n";

    XpuSendRecvMiXnodeHello()
        : TestApp("xpuSendRecvMiXnodeHello",
                  App::HostType::Python2,
                  App::FlagNone,
                  Source)
    {
    }
    virtual Status execute();
};

class XpuSendRecvSiLarge : public TestApp
{
  public:
    static constexpr const char *Source =
        "# Single Instance App with XPU 0 sending large payload to last XPU  \n"
        "# Since it's SingleInstance, dest XPU will be on another XCE Node   \n"
        "# Check the MD5 hash for the payload before and after transmission  \n"
        "# One more transmission is done b/w the two xpus with roles reversed\n"
        "import xcalar.container.xpuComm as xpuComm                      \n"
        "import sys                                                          \n"
        "import xcalar.container.xpu_host as xpu_host                        \n"
        "import hashlib                                                      \n"
        "def main(inBlob):                                                   \n"
        "    thisXpuId = xpu_host.get_xpu_id()                             \n"
        "    xpuClusSz = xpu_host.get_xpu_cluster_size()                    \n"
        "    # send 2MB message filled with the char 'a'                     \n"
        "    hugelist = ['a'] * 2097152                                      \n"
        "    hugestr = ''.join(hugelist)                                     \n"
        "    hashSent = hashlib.md5(hugestr)                                 \n"
        "    dstXpu = xpuClusSz - 1                                          \n"
        "    if xpuClusSz == 1:                                              \n"
        "        return 'not engaging' + '\\n'                               \n"
        "    if thisXpuId == 0:                                              \n"
        "        xpuComm.xpuSend(dstXpu, hugestr.encode('utf-8'))            \n"
        "        msg = xpuComm.xpuRecv().decode('utf-8')                     \n"
        "        hashRcvd = hashlib.md5(msg)                                 \n"
        "        if hashSent.hexdigest() != hashRcvd.hexdigest():            \n"
        "            print(                                                  \n"
        "           'ERROR: Xpu {} rcv msg hash mismatch!'.format(thisXpuId),\n"
        "            file=sys.stderr)                                        \n"
        "        return 'sent and then received large message' + ' \\n'      \n"
        "    elif thisXpuId == dstXpu:                                       \n"
        "        msg = xpuComm.xpuRecv().decode('utf-8')                     \n"
        "        hashRcvd = hashlib.md5(msg)                                 \n"
        "        if hashSent.hexdigest() != hashRcvd.hexdigest():            \n"
        "            print(                                                  \n"
        "           'ERROR: Xpu {} rcv msg hash mismatch!'.format(thisXpuId),\n"
        "            file=sys.stderr)                                        \n"
        "        xpuComm.xpuSend(0, hugestr.encode('utf-8'))                 \n"
        "        return 'received and then sent a large message' + '\\n'     \n"
        "    else:                                                           \n"
        "        return 'not engaging' + '\\n'                              \n";

    XpuSendRecvSiLarge()
        : TestApp("xpuSendRecvSiLarge",
                  App::HostType::Python2,
                  App::FlagInstancePerNode,
                  Source)
    {
    }
    virtual Status execute();
};

class XpuSendRecvMiLarge : public TestApp
{
  public:
    static constexpr const char *Source =
        "# Multi Instance App with XPU 0 sending large payload to XPU 1      \n"
        "# Since it's MultiInstance, a XCE node will have numCores XPUs      \n"
        "# which will typically = 4/8 XPUs- so XPU 1 will be on same XCE Node\n"
        "# Check the MD5 hash for the payload before and after transmission  \n"
        "# One more transmission is done b/w the two xpus with roles reversed\n"
        "import xcalar.container.xpuComm as xpuComm                      \n"
        "import sys                                                          \n"
        "import xcalar.container.xpu_host as xpu_host                        \n"
        "import hashlib                                                      \n"
        "def main(inBlob):                                                   \n"
        "    thisXpuId = xpu_host.get_xpu_id()                             \n"
        "    xpuClusSz = xpu_host.get_xpu_cluster_size()                    \n"
        "    # send 2MB message filled with the char 'a'                     \n"
        "    hugelist = ['a'] * 2097152                                      \n"
        "    hugestr = ''.join(hugelist)                                     \n"
        "    hashSent = hashlib.md5(hugestr)                                 \n"
        "    if xpuClusSz == 1:                                              \n"
        "        return 'not engaging' + '\\n'                               \n"
        "    if thisXpuId == 0:                                              \n"
        "        xpuComm.xpuSend(1, hugestr.encode('utf-8'))                 \n"
        "        msg = xpuComm.xpuRecv().decode('utf-8')                     \n"
        "        hashRcvd = hashlib.md5(msg.decode('utf-8'))                 \n"
        "        if hashSent.hexdigest() != hashRcvd.hexdigest():            \n"
        "            print(                                                  \n"
        "           'ERROR: Xpu {} rcv msg hash mismatch!'.format(thisXpuId),\n"
        "            file=sys.stderr)                                        \n"
        "        return 'sent and received 1 large message' + ' \\n'         \n"
        "    elif thisXpuId == 1:                                            \n"
        "        msg = xpuComm.xpuRecv().decode('utf-8')                     \n"
        "        hashRcvd = hashlib.md5(msg)                                 \n"
        "        if hashSent.hexdigest() != hashRcvd.hexdigest():            \n"
        "            print(                                                  \n"
        "           'ERROR: Xpu {} rcv msg hash mismatch!'.format(thisXpuId),\n"
        "            file=sys.stderr)                                        \n"
        "        xpuComm.xpuSend(0, hugestr.encode('utf-8'))                 \n"
        "        return 'received and sent 1 large message' + '\\n'          \n"
        "    else:                                                           \n"
        "        return 'not engaging' + '\\n'                              \n";

    XpuSendRecvMiLarge()
        : TestApp("xpuSendRecvMiLarge",
                  App::HostType::Python2,
                  App::FlagNone,
                  Source)
    {
    }
    virtual Status execute();
};

class XpuSendRecvMiXnodeLarge : public TestApp
{
  public:
    static constexpr const char *Source =
        "# Multi instance App: XPU 0 sends huge payload to XPU on diff node  \n"
        "# Since it's MultiInstance, a XCE node will have numCores XPUs      \n"
        "# and XPU cluster size = nodeCount * xpusPerNode, so XPU on a diff  \n"
        "# node will be at least XPU 0 + xpusPerNode away                    \n"
        "# Check the MD5 hash for the payload before and after transmission  \n"
        "# One more transmission is done b/w the two xpus with roles reversed\n"
        "import xcalar.container.xpuComm as xpuComm                      \n"
        "import sys                                                          \n"
        "import xcalar.container.xpu_host as xpu_host                        \n"
        "import xcalar.container.context as xpu                          \n"
        "import hashlib                                                      \n"
        "def main(inBlob):                                                   \n"
        "    thisXpuId = xpu_host.get_xpu_id()                             \n"
        "    xpuClusSz = xpu_host.get_xpu_cluster_size()                    \n"
        "    # send 2MB message filled with the char 'a'                     \n"
        "    hugelist = ['a'] * 2097152                                      \n"
        "    hugestr = ''.join(hugelist)                                     \n"
        "    hashSent = hashlib.md5(hugestr)                                 \n"
        "    destXpuId = xpuClusSz - 1                                       \n"
        "    if xpuClusSz == 1:                                              \n"
        "        return 'not engaging' + '\\n'                               \n"
        "    if thisXpuId == 0:                                              \n"
        "        xpuComm.xpuSend(destXpuId, hugestri.encode('utf-8'))        \n"
        "        msg = xpuComm.xpuRecv().decode('utf-8')                     \n"
        "        hashRcvd = hashlib.md5(msg)                                 \n"
        "        if hashSent.hexdigest() != hashRcvd.hexdigest():            \n"
        "            print(                                                  \n"
        "           'ERROR: Xpu {} rcv msg hash mismatch!'.format(thisXpuId),\n"
        "            file=sys.stderr)                                        \n"
        "        return 'sent and received 1 large message' + ' \\n'         \n"
        "    elif thisXpuId == destXpuId:                                    \n"
        "        msg = xpuComm.xpuRecv().decode('utf-8')                     \n"
        "        hashRcvd = hashlib.md5(msg)                                 \n"
        "        if hashSent.hexdigest() != hashRcvd.hexdigest():            \n"
        "            print(                                                  \n"
        "           'ERROR: Xpu {} rcv msg hash mismatch!'.format(thisXpuId),\n"
        "            file=sys.stderr)                                        \n"
        "        xpuComm.xpuSend(0, hugestr)                                 \n"
        "        return 'received and sent 1 large message' + '\\n'          \n"
        "    else:                                                           \n"
        "        return 'not engaging' + '\\n'                              \n";

    XpuSendRecvMiXnodeLarge()
        : TestApp("xpuSendRecvMiXnodeLarge",
                  App::HostType::Python2,
                  App::FlagNone,
                  Source)
    {
    }
    virtual Status execute();
};

class XpuSendRecvListSiHello : public TestApp
{
  public:
    static constexpr const char *Source =
        "# Single Instance with XPU 0 fanning out tiny payload to all XPUs   \n"
        "# Since it's SingleInstance, receivers will be different XCE Nodes  \n"
        "import xcalar.container.xpuComm as xpuComm                      \n"
        "import xcalar.container.xpu_host as xpu_host                        \n"
        "def main(inBlob):                                                   \n"
        "    thisXpuId = xpu_host.get_xpu_id()                             \n"
        "    xpuClusSz = xpu_host.get_xpu_cluster_size()                    \n"
        "    sendList = []                                                   \n"
        "    if xpuClusSz == 1:                                              \n"
        "        return 'not engaging' + '\\n'                               \n"
        "    if thisXpuId == 0:                                              \n"
        "        for i in range(xpuClusSz - 1):                              \n"
        "            dstTuple = (i+1, 'fanout hello world from xpu 0')       \n"
        "            sendList.append(dstTuple)                               \n"
        "        xpuComm.xpuSendFanout(sendList)                             \n"
        "        return 'sent messages in a fanout manner' + '\\n'           \n"
        "    else:                                                           \n"
        "        msg = xpuComm.xpuRecv()                                     \n"
        "        return 'received 1 message' + '\\n'                        \n";

    XpuSendRecvListSiHello()
        : TestApp("xpuSendRecvListSiHello",
                  App::HostType::Python2,
                  App::FlagInstancePerNode,
                  Source)
    {
    }
    virtual Status execute();
};

class XpuSendRecvListMiLarge : public TestApp
{
  public:
    static constexpr const char *Source =
        "# Multi Instance with XPU 0 sending large payload to all XPUs       \n"
        "# Since it's Multi Instance, and this is fanout, receivers could be \n"
        "# on different or the same XCE Nodes                                \n"
        "import xcalar.container.xpuComm as xpuComm                      \n"
        "import sys                                                          \n"
        "import xcalar.container.xpu_host as xpu_host                        \n"
        "import hashlib                                                      \n"
        "def main(inBlob):                                                   \n"
        "    thisXpuId = xpu_host.get_xpu_id()                             \n"
        "    xpuClusSz = xpu_host.get_xpu_cluster_size()                    \n"
        "    if xpuClusSz == 1:                                              \n"
        "        return 'not engaging' + '\\n'                               \n"
        "    # send 2MB message filled with the char 'a'                     \n"
        "    hugelist = ['a'] * 2097152                                      \n"
        "    hugestr = ''.join(hugelist)                                     \n"
        "    hashSent = hashlib.md5(hugestr)                                 \n"
        "    sendList = []                                                   \n"
        "    if thisXpuId == 0:                                              \n"
        "        for i in range(xpuClusSz - 1):                              \n"
        "            dstTuple = (i+1, hugestr.encode('utf-8'))               \n"
        "            sendList.append(dstTuple)                               \n"
        "        xpuComm.xpuSendFanout(sendList)                             \n"
        "        return 'sent large messages in a fanout manner' + '\\n'     \n"
        "    else:                                                           \n"
        "        msg = xpuComm.xpuRecv().decode('utf-8')                     \n"
        "        hashRcvd = hashlib.md5(msg)                                 \n"
        "        if hashSent.hexdigest() != hashRcvd.hexdigest():            \n"
        "            print(                                                  \n"
        "           'ERROR: Xpu {} rcv msg hash mismatch!'.format(thisXpuId),\n"
        "            file=sys.stderr)                                        \n"
        "        return 'received 1 large message' + '\\n'                  \n";

    XpuSendRecvListMiLarge()
        : TestApp("xpuSendRecvListMiLarge",
                  App::HostType::Python2,
                  App::FlagNone,
                  Source)
    {
    }
    virtual Status execute();
};

class GetUserNameTest : public TestApp
{
  public:
    static constexpr const char *Source =
        "import xcalar.container.context as ctx                          \n"
        "def main(inBlob):                                                   \n"
        "    uname = ctx.getUserIdName()                                     \n"
        "    return uname                                                   \n";
    GetUserNameTest()
        : TestApp("getUserNameTest",
                  App::HostType::Python2,
                  App::FlagInstancePerNode,
                  Source)
    {
    }
    virtual Status execute();
};

class AppFuncTests : public FuncTestBase
{
  public:
    // Test entry points.
    static Status testSanity();
    static Status testStress();
    static Status testCustom();

    static Status parseConfig(Config::Configuration *config,
                              char *key,
                              char *value,
                              bool stringentRules);

    Status userMain(FuncTestBase::User *me) override;

  private:
    static constexpr const char *ModuleName = "libapp::functests";

    Status test();
    Status createTestApps();
    Status littleTests(FuncTestBase::User *me);

    RandHandle rnd_;

    enum class AppName : uint32_t {
        BigOutput,
        Module,
        Errors,
        Pause,
        Echo,
        BarrierSi,
        BarrierStressSi,
        BarrierStressMi,
        BarrierSendRecvSi,
        BarrierSendRecvMi,
        BarrierMi,
        BarrierSiRand,
        BarrierMiRand,
        BarrierMiFailure,
        StartFailure,
        XpuSendRecvSiHello,
        XpuSendRecvMiHello,
        XpuSendRecvMiXnodeHello,
        XpuSendRecvSiLarge,
        XpuSendRecvMiLarge,
        XpuSendRecvMiXnodeLarge,
        XpuSendRecvListSiHello,
        XpuSendRecvListMiLarge,
        GetUserNameTest,
        AppNameLast,  // Must be the last entry here.
    };

    enum class AppMgmtOpTests : uint32_t {
        AppOpen,
        AppCreate,
        AppRemove,
        AppUpdate,
        AppMgmtOpLast,
    };

    static constexpr const uint32_t AppTestCaseCount =
        (const uint32_t) AppName::AppNameLast;

    TestApp *apps_[AppTestCaseCount];

    static AppName AppSanity[];
    static AppName AppStress[];

    enum class AppTestType {
        Sanity,
        Stress,
    };

  public:
    struct Params {
        // Total threads involved in running the test.
        unsigned totalThreadCount;

        // How many times to run random little sanity/edge tests *per thread*.
        unsigned littleTestIters;

        AppTestType testType;
    };

    static constexpr Params paramsSanity = {
        .totalThreadCount = 3,
        .littleTestIters = 100,
        .testType = AppTestType::Sanity,
    };

    static constexpr Params paramsStress = {
        .totalThreadCount = 7,
        .littleTestIters = 100,
        .testType = AppTestType::Stress,
    };

    AppFuncTests(const Params &params);
    ~AppFuncTests();

  private:
    const Params p_;
    static Params paramsCustom;  // Used for testCustom.
};

#endif  // APPFUNCTESTS_H
