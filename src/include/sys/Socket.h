// Copyright 2013 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _SOCKET_H_
#define _SOCKET_H_

#include <time.h>
#include "SocketTypes.h"
#include "sys/SgTypes.h"

// Only call this if you're calling connect directly.
// This is performed automatically if you use sockConnect
MustCheck Status sockInitiateHandshake(SocketHandle sockHandle);

MustCheck Status sockCreate(const char *hostName,
                            uint16_t port,
                            SocketDomain domain,
                            SocketType type,
                            SocketHandle *sockHandle,
                            SocketAddr *sockAddr) NonNull(5);

//!
//! \brief Destroy a socket
//!
//! Destroy a formerly created socket
//!
//! \param[in,out] sockHandle handle to socket which to destroy
//!
//! \pre it is the caller's responsibility to have previously created the
//!      socket with sockCreate() or accepted it with sockAccept()
//!
void sockDestroy(SocketHandle *sockHandle) NonNull(1);

//!
//! \brief Set socket option
//!
//! Set an option on the specified socket
//!
//! \param[in] sockHandle socket handle to set the option on
//! \param[in] option option to set on the socket
//!
//! \retval StatusOk the option was set successfully
//! \retval StatusXXX a failure occurred
//!
//! \pre it is the caller's responsibility to have previously created the
//!      socket with sockCreate() or accepted it with sockAccept()
//!
Status sockSetOption(SocketHandle sockHandle, SocketOption option);

// set recv timeout on a socket descriptor
Status sockSetRecvTimeout(SocketHandle sockHandle, uint64_t timeoutInUSeconds);

//!
//! \brief Get socket address
//!
//! Get a socket address from the specified hostName, port, domain, and type.
//! \note This interface was modeled after POSIX' getaddrinfo().  See
//! GETADDRINFO(3) manpage for additional detail.  You can find example usage
//! of this interface in socketTestServer() and socketTests().
//!
//! \param[in] hostName hostname whose IP address to obtain or for bind sockets
//!            the string IP address representing the interface to bind the
//!            socket to.  Alternatively for bind sockets the caller may use
//!            SockIPAddrAny.
//! \param[in] port IP port to set in the address
//! \param[in] domain socket domain to initialize in the address
//! \param[in] type socket type to initialize in the address
//! \param[out] sockAddr socket address to initialize
//!
//! \retval StatusOk the socket address was initialized
//! \retval StatusXXX a failure occurred
//!
MustCheck Status sockGetAddr(const char *hostName,
                             uint16_t port,
                             SocketDomain domain,
                             SocketType type,
                             SocketAddr *sockAddr) NonNull(5);

//!
//! \brief Connect the socket to a server
//!
//! Connect the socket to the specified server.
//!
//! \param[in] sockHandle socket handle to connect
//! \param[in] sockAddr address of the server to connect to
//!
//! \retval StatusOk the connection successfully established
//! \retval StatusXXX a failure occurred
//!
//! \pre it is the caller's responsibility to have previously created the
//!      socket with sockCreate()
//! \pre it is the caller's responsibility to have previously initialized the
//!      socket address with sockGetAddr()
//!
MustCheck Status sockConnect(SocketHandle sockHandle,
                             const SocketAddr *sockAddr) NonNull(2);

//!
//! \brief Bind the socket
//!
//! Bind the socket to the specified ip adress.
//!
//! \param[in] sockHandle socket handle to bind
//! \param[in] sockAddr address to bind the socket to
//!
//! \retval StatusOk the socket was successfully bound
//! \retval StatusXXX a failure occurred
//!
//! \pre it is the caller's responsibility to have previously created the
//!      socket with sockCreate()
//! \pre it is the caller's responsibility to have previously initialized the
//!      socket address with sockGetAddr()
//!
MustCheck Status sockBind(SocketHandle sockHandle, const SocketAddr *sockAddr)
    NonNull(2);

//!
//! \brief Set socket listener depth
//!
//! Set the maximum number of clients the server will allow to block
//! in connect() while waiting for the server to accept().
//!
//! \param[in] sockHandle socket handle to set listener depth
//! \param[in] maxClientsWaiting max depth to set
//!
//! \retval StatusOk listener depth successfully set
//! \retval StatusXXX a failure occurred
//!
//! \pre it is the caller's responsibility to have previously bound the
//!      socket with sockBind()
//!
MustCheck Status sockListen(SocketHandle sockHandle,
                            unsigned maxClientsWaiting);

//!
//! \brief Accept new client connect
//!
//! Accept a new connection from a client
//!
//! \param[in] masterHandle master bound server socket to accept from
//! \param[out] newClientAddr socket address of the connecting client or
//!             NULL if the caller doesn't care.
//! \param[out] newClientHandle newly created client socket
//!
//! \retval StatusOk client successfully connected
//! \retval StatusXXX a failure occurred
//!
//! \pre it is the caller's responsibility to have previously listened on the
//!      master socket with sockListen()
//! \post it is the caller's responsibility to invoke sockDestroy()
//!       to cleanup the new client socket when finished working with it
//!
MustCheck Status sockAccept(SocketHandle masterHandle,
                            SocketAddr *newClientAddr,
                            SocketHandle *newClientHandle) NonNull(3);

//!
//! \brief Send data on a socket
//!
//! Send data on a connected socket.
//! \note a return of StatusOk only indicates *some* data was sent, not
//!       *all*.  Callers should check for the number of bytes sent in
//!       bytesSent after testing the return value.  See READV(2) manpage
//!       for further discussion.
//!
//! \param[in] sockHandle socket to send data on
//! \param[in] sgArray scatter/gather array pointing to the data buffer(s) to
//!            be sent
//! \param[in] flags https://linux.die.net/man/2/sendmsg
//! \param[out] bytesSent number of bytes sent
//!
//! \retval StatusOk data successfully sent
//! \retval StatusXXX a failure occurred
//!
//! \pre it is the caller's responsibility to have either previously accepted
//!      the socket via sockAccept() or conneted the socket via sockConnect()
//!
MustCheck Status sockSendV(SocketHandle sockHandle,
                           SgArray *sgArray,
                           size_t *bytesSent,
                           int flags) NonNull(2, 3);

//!
//! \brief Send data on a socket
//!
//! Send data on a connected socket.
//! \note a return of StatusOk indicates *all* data was sent.
//!
//! \param[in] sockHandle socket to send data on
//! \param[in] buf the data buffer to be sent
//! \param[in] bufLen the number of bytes to send
//!
//! \retval StatusOk data successfully sent
//! \retval StatusXXX a failure occurred
//!
//! \pre it is the caller's responsibility to have either previously accepted
//!      the socket via sockAccept() or conneted the socket via sockConnect()
//!
MustCheck Status sockSendConverged(SocketHandle sockHandle,
                                   void *buf,
                                   size_t bufLen) NonNull(2);

//!
//! \brief Recieve data from a socket
//!
//! Recieve data from on a connected socket.
//! \note a return of StatusOk only indicates *some* data was recieved, not
//!       *all*.  Callers should check for the number of bytes recieved in
//!       bytesRecv after testing the return value.  See READV(2) manpage
//!       for further discussion.
//!
//! \param[in] sockHandle socket to recieve data from
//! \param[in] sgArray scatter/gather array pointing to the data buffer(s) to
//!            receive into
//! \param[out] bytesRecv number of bytes recieved
//!
//! \retval StatusOk data successfully sent
//! \retval StatusXXX a failure occurred
//!
//! \pre it is the caller's responsibility to have either previously accepted
//!      the socket via sockAccept() or conneted the socket via sockConnect()
//!
MustCheck Status sockRecvV(SocketHandle sockHandle,
                           SgArray *sgArray,
                           size_t *bytesRecv) NonNull(2, 3);

//!
//! \brief Recieve data from a socket
//!
//! Recieve data from a a connected socket.
//! \note a return of StatusOk indicates *all* data was recieved.
//!
//! \param[in] sockHandle socket to send data on
//! \param[out] buf the data buffer to read into
//! \param[in] bufLen the number of bytes to receive
//! \param[out] bytesRecv number of bytes received
//!
//! \retval StatusOk data successfully sent
//! \retval StatusXXX a failure occurred
//!
//! \pre it is the caller's responsibility to have either previously accepted
//!      the socket via sockAccept() or conneted the socket via sockConnect()
//!
MustCheck Status sockRecvConverged(SocketHandle sockHandle,
                                   void *buf,
                                   size_t bufLen) NonNull(2);

//!
//! \brief Initialize a socket handle set
//!
//! Initialize a socket handle set (a.k.a. FD_ZERO())
//!
//! \param[out] handleSet socket handle set to initialize
//!
void sockHandleSetInit(SocketHandleSet *handleSet) NonNull(1);

//!
//! \brief Add a socket handle to a set
//!
//! Add a socket handle to a socket handle set.  (a.k.a. FD_SET())
//!
//! \param[in,out] handleSet socket handle set to add to
//! \param[in] sockHandle socket handle to add
//!
//! \pre handleSet must have been previously initialized with
//!      sockHandleSetInit()
//!
void sockHandleSetAdd(SocketHandleSet *handleSet, SocketHandle sockHandle)
    NonNull(1);

//!
//! \brief Remove a socket handle from a set
//!
//! Remove a socket handle from a socket handle set.  (a.k.a. FD_CLR())
//!
//! \param[in] handleSet socket handle set to remove from
//! \param[in] sockHandle socket handle to remove
//!
//! \pre handleSet must have been previously initialized with
//!      sockHandleSetInit()
//!
void sockHandleSetRemove(SocketHandleSet *handleSet, SocketHandle sockHandle)
    NonNull(1);

//!
//! \brief Determine if a set contains a handle
//!
//! Determine if a socket handle set contains a handle.  (a.k.a. FD_ISSET())
//!
//! \param[in] handleSet socket handle set to test
//! \param[in] sockHandle socket handle to test for
//!
//! \retval true handleSet contains sockHandle
//! \retval false handleSet does not contain sockHandle
//!
//! \pre handleSet must have been previously initialized with
//!      sockHandleSetInit()
//!
MustCheck bool sockHandleSetIncludes(const SocketHandleSet *handleSet,
                                     SocketHandle sockHandle) NonNull(1);

//!
//! \brief Block & wait until there is work to do on some sockets
//!
//! This function will block the calling thread until 1 of the following 4
//! conditions occurs:
//! 1.  at least 1 socket within readHandles is ready to be read from
//! 2.  at least 2 socket within writeHandles is ready to be written to
//! 3.  at least 2 socket within exceptHandles is ready to be cleared up
//! 4.  timeout has expired
//! Upon return, the number of sockets that are ready are returned via
//! numReady if specified.
//!
//! \param[in] readHandles (optional) read socket handle set to test
//! \param[in] writeHandles (optional) write socket handle set to test
//! \param[in] exceptHandles (optional) except handle set to test
//! \param[in] timeout (optional) maximum length of time to block
//! \param[out] numReady (optional) number of sockets that are ready
//!
//! \retval StatusOk the function completed successfully
//! \retval StatusXXX an error occurred
//!
//! \pre any non-NULL readHandles, writeHandles, exceptHandles must have been
//!      previously initialized with sockHandleSetInit()
//!
MustCheck Status sockSelect(const SocketHandleSet *readHandles,
                            const SocketHandleSet *writeHandles,
                            const SocketHandleSet *exceptHandles,
                            const struct timespec *timeout,
                            unsigned *numReady);

// XXX FIXME hack temporary interface
SocketHandle sockHandleFromInt(int fd);

#endif  // _SOCKET_H_
