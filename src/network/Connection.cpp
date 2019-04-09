/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2018 SciDB, Inc.
* All Rights Reserved.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

/*
 * Connection.cpp
 *
 *  Created on: Jan 15, 2010
 *      Author: roman.simakov@gmail.com
 */

#include "Connection.h"

#include "NetworkManager.h"

#include <system/Auth.h>
#include <system/Config.h>
#include <system/Exceptions.h>
#include <util/Notification.h>
#include <util/Timing.h>
#include <rbac/Session.h>
#include <rbac/SessionProperties.h>

#include <log4cxx/logger.h>
#include <boost/bind.hpp>
#include <memory>

using namespace std;
using namespace boost;

namespace scidb
{
// Logger for network subsystem. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network"));

/***
 * C o n n e c t i o n
 */
Connection::Connection(
    NetworkManager& networkManager,
    uint64_t genId,
    InstanceID selfInstanceId,
    InstanceID instanceID):
    BaseConnection(networkManager.getIOService()),
    _messageQueue(instanceID, *this),
    _networkManager(networkManager),
    _remoteInstanceId(instanceID),
    _selfInstanceId(selfInstanceId),
    _sessPri(SessionProperties::NORMAL),
    _genId(genId),
    _connectionState(NOT_CONNECTED),
    _remotePort(0),
    _isSending(false),
    _logConnectErrors(true),
    _flushThenDisconnect(false),
    _lastRecv(getCoarseTimestamp())
{
   assert(selfInstanceId != INVALID_INSTANCE);
}

void Connection::start()
{
    assert(!_error);

    // Direct transition to AUTH_IN_PROGRESS since we are the server end.
    assert(_connectionState == NOT_CONNECTED);
    _connectionState = AUTH_IN_PROGRESS;

    setRemoteIpAndPort();

    LOG4CXX_DEBUG(logger, "Connection received from " << getPeerId());

    // The first work we should do is reading initial message from client
    _networkManager.getIOService().post(bind(&Connection::readMessage,
                                             shared_from_this()));
}

/*
 * @brief Read from socket sizeof(MessageHeader) bytes of data to _messageDesc.
 */
void Connection::readMessage()
{
   LOG4CXX_TRACE(logger, "Reading next message header");

   assert(!_messageDesc);
   _messageDesc = make_shared<ServerMessageDesc>();
   // XXX TODO: add a timeout after we get the first byte
   asio::async_read(_socket,
                    asio::buffer(&_messageDesc->getMessageHeader(),
                                 sizeof(_messageDesc->getMessageHeader())),
                    bind(&Connection::handleReadMessage, shared_from_this(),
                         asio::placeholders::error,
                         asio::placeholders::bytes_transferred));
}

/**
 * @brief Validate _messageDesc & read from socket
 *    _messageDesc->getMessageHeader().getRecordSize() bytes of data
 */
void Connection::handleReadMessage(
    const boost::system::error_code& error,
    size_t bytes_transferr)
{
   if (error)
   {
      handleReadError(error);
      return;
   }

   if(!_messageDesc->validate() ||
      _messageDesc->getMessageHeader().getSourceInstanceID() == _selfInstanceId) {
      LOG4CXX_ERROR(logger, "Connection::handleReadMessage: unknown/malformed message,"
                            " closing connection");
      using namespace boost::asio::error;
      handleReadError(make_error_code(eof));
      return;
   }

   assert(bytes_transferr == sizeof(_messageDesc->getMessageHeader()));
   assert(_messageDesc->getMessageHeader().getSourceInstanceID() != _selfInstanceId);
   assert(_messageDesc->getMessageHeader().getNetProtocolVersion() == scidb_msg::NET_PROTOCOL_CURRENT_VER);

   async_read(_socket,
              _messageDesc->_recordStream.prepare(
                  _messageDesc->getMessageHeader().getRecordSize()),
              bind(&Connection::handleReadRecordPart,
                   shared_from_this(),  asio::placeholders::error,
                   asio::placeholders::bytes_transferred));

   LOG4CXX_TRACE(logger, "Connection::handleReadMessage: "
            << strMsgType(_messageDesc->getMessageHeader().getMessageType())
            << " from instanceID="
            << Iid(_messageDesc->getMessageHeader().getSourceInstanceID())
            << " ; recordSize="
            << _messageDesc->getMessageHeader().getRecordSize()
            << " ; messageDesc.binarySize="
            << _messageDesc->getMessageHeader().getBinarySize());
}

/*
 * @brief  If header indicates data is available, read from
 *         socket _messageDesc->binarySize bytes of data.
 */
void Connection::handleReadRecordPart(
    const boost::system::error_code& error,
    size_t bytes_transferr)
{
   if (error)
   {
      handleReadError(error);
      return;
   }

   assert(_messageDesc->validate());

   assert(  _messageDesc->getMessageHeader().getRecordSize() ==
            bytes_transferr);

   assert(  _messageDesc->getMessageHeader().getSourceInstanceID() !=
            _selfInstanceId);

   if (!_messageDesc->parseRecord(bytes_transferr)) {
       LOG4CXX_ERROR(logger,
                     "Network error in handleReadRecordPart: cannot parse record for "
                     << " msgID="
                     << _messageDesc->getMessageHeader().getMessageType()
                     << ", closing connection");

       using boost::asio::error::make_error_code;
       using boost::asio::error::eof;
       handleReadError(make_error_code(eof));
       return;
   }
   _messageDesc->prepareBinaryBuffer();

   LOG4CXX_TRACE(logger,
        "handleReadRecordPart: "
            << strMsgType(_messageDesc->getMessageHeader().getMessageType())
            << " ; messageDesc.binarySize="
            << _messageDesc->getMessageHeader().getBinarySize());

   if (_messageDesc->_messageHeader.getBinarySize())
   {
      async_read(_socket,
            asio::buffer(
                _messageDesc->_binary->getWriteData(),
                _messageDesc->_binary->getSize()),
                bind(
                    &Connection::handleReadBinaryPart,
                    shared_from_this(),
                    asio::placeholders::error,
                    asio::placeholders::bytes_transferred));
      return;
   }

   handleReadBinaryPart(error, 0);
}

/*
 * @brief Invoke the appropriate dispatch routine
 */
void Connection::handleReadBinaryPart(
    const boost::system::error_code& error,
    size_t bytes_transferr)
{
   if (error)
   {
      handleReadError(error);
      return;
   }
   assert(_messageDesc);

   assert(_messageDesc->getMessageHeader().getBinarySize() ==
          bytes_transferr);

   std::shared_ptr<MessageDesc> msgPtr;
   _messageDesc.swap(msgPtr);

   _lastRecv = getCoarseTimestamp();

   // mtHangup means we need not read from this connection again, it
   // is for outbound peer traffic.  (We did read from it during
   // authentication, but we are done now.)
   //
   // NOTE, however, that because of SDB-5702 mtHangup is not
   // currently being used.
   //
   if (msgPtr->getMessageType() == mtHangup) {
       if (isOutbound()) {
           LOG4CXX_TRACE(logger, "Hanging up on outbound connection to "
                         << getPeerId() << " on conn=" << hex << this << dec);
           // We just need to NOT queue up another readMessage() call.
           return;
       }
       LOG4CXX_DEBUG(logger, "DROPPED mtHangup from non-peer " << getPeerId()
                     << " on conn=" << hex << this << dec);
   }
   else {
       std::shared_ptr<Connection> self(shared_from_this());
       _networkManager.handleMessage(self, msgPtr);
   }

   // Preparing to read new message
   assert(_messageDesc.get() == NULL);
   readMessage();
}

void Connection::sendMessage(std::shared_ptr<MessageDesc>& messageDesc,
                             NetworkManager::MessageQueueType mqt)
{
    pushMessage(messageDesc, mqt);
   _networkManager.getIOService().post(bind(&Connection::pushNextMessage,
                                            shared_from_this()));
}

void Connection::sendMessageDisconnectOnError(std::shared_ptr<MessageDesc>& m,
                                              NetworkManager::MessageQueueType mqt)
{
    try {
        sendMessage(m, mqt);
    }
    catch (const scidb::Exception& e) {
        try { disconnect(); } catch (...) {}
        throw;
    }
}

void Connection::pushMessage(std::shared_ptr<MessageDesc>& messageDesc,
                             NetworkManager::MessageQueueType mqt)
{
    std::shared_ptr<const NetworkManager::ConnectionStatus> connStatus;
    {
        ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);

        LOG4CXX_TRACE(logger, "pushMessage: send message queue size = "
                      << _messageQueue.size()
                      << " for instanceID=" << Iid(_remoteInstanceId)
                      << " flushThenDisconnect=" << _flushThenDisconnect
                      << " conn=" << hex << this << dec);

        if(_flushThenDisconnect)
        {
            // Do not allow new messages.
            throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_CONNECTION_ERROR2)
                       << "(not connected)" << "pending disconnect after flush";
        }

        if (isAuthenticating() && isAuthMessage(messageDesc->getMessageType()))
        {
            // Auth handshake must happen first!
            LOG4CXX_TRACE(logger, "pushMessage: pushing an auth msg");
            connStatus = _messageQueue.pushUrgent(messageDesc);
        } else {
            LOG4CXX_TRACE(logger, "pushMessage: pushing a msg for mqt=" << mqt);
            connStatus = _messageQueue.pushBack(mqt, messageDesc);
        }
        publishQueueSizeIfNeeded(connStatus);
    }
    if (connStatus) {
        LOG4CXX_TRACE(logger, "pushMessage: will publishQueueSize");
        _networkManager.getIOService().post(bind(&Connection::publishQueueSize,
                                                 shared_from_this()));
    }
}

std::shared_ptr<MessageDesc> Connection::popMessage()
{
    std::shared_ptr<const NetworkManager::ConnectionStatus> connStatus;
    std::shared_ptr<MessageDesc> msg;
    {
        ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);

        connStatus = _messageQueue.popFront(msg);
        // If popped msg while authenticating, then no connStatus
        // (because auth happens on non-flow-controlled channel).
        SCIDB_ASSERT(!msg || !isAuthenticating() || !connStatus);
        publishQueueSizeIfNeeded(connStatus);
    }
    if (connStatus) {
        _networkManager.getIOService().post(bind(&Connection::publishQueueSize,
                                                 shared_from_this()));
    }
    if (logger->isTraceEnabled() && msg) {
        LOG4CXX_TRACE(logger, "popMessage: popped "
                      << strMsgType(msg->getMessageType())
                      << " from conn=" << hex << this << dec);
    }
    return msg;
}

void Connection::setRemoteQueueState(NetworkManager::MessageQueueType mqt,  uint64_t size,
                                     uint64_t localGenId, uint64_t remoteGenId,
                                     uint64_t localSn, uint64_t remoteSn)
{
    assert(mqt != NetworkManager::mqtNone);
    std::shared_ptr<const NetworkManager::ConnectionStatus> connStatus;
    {
        ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);

        connStatus = _messageQueue.setRemoteState(mqt, size,
                                                  localGenId, remoteGenId,
                                                  localSn, remoteSn);

        LOG4CXX_TRACE(logger, "setRemoteQueueSize: remote queue size = "
                      << size <<" for instanceID=" << Iid(_remoteInstanceId)
                      << " for queue " << mqt
                      << ", conn=" << hex << this << dec);

        publishQueueSizeIfNeeded(connStatus);
    }
    if (connStatus) {
        _networkManager.getIOService().post(bind(&Connection::publishQueueSize,
                                                 shared_from_this()));
    }
    _networkManager.getIOService().post(bind(&Connection::pushNextMessage,
                                             shared_from_this()));
}

bool
Connection::publishQueueSizeIfNeeded(const std::shared_ptr<const NetworkManager::ConnectionStatus>& connStatus)
{
    SCIDB_ASSERT(_mutex.isLockedByThisThread());

    if (!connStatus) {
        return false;
    }
    _statusesToPublish[connStatus->getQueueType()] = connStatus;
    return true;
}

void Connection::publishQueueSize()
{
    ConnectionStatusMap toPublish;
    {
        ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);
        toPublish.swap(_statusesToPublish);
    }
    for (ConnectionStatusMap::iterator iter = toPublish.begin();
         iter != toPublish.end(); ++iter) {

        std::shared_ptr<const NetworkManager::ConnectionStatus>& status = iter->second;
        NetworkManager::MessageQueueType mqt = iter->first;
        assert(mqt == status->getQueueType());
        assert(mqt != NetworkManager::mqtNone);
        assert(mqt < NetworkManager::mqtMax);
        LOG4CXX_TRACE(logger, "FlowCtl: PUB iid=" << Iid(_remoteInstanceId)
                      << " avail=" << status->getAvailableQueueSize()
                      << " mqt=" << mqt);
        assert(_remoteInstanceId == status->getPhysicalInstanceId());

        Notification<NetworkManager::ConnectionStatus> event(status);
        event.publish();
    }
}

size_t Connection::queueSize()
{
    ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);
    return _messageQueue.size();
}

void Connection::handleSendMessage(const boost::system::error_code& error,
                                   size_t bytes_transferred,
                                   std::shared_ptr< std::list<std::shared_ptr<MessageDesc> > >& msgs,
                                   size_t bytes_sent)
{
   _isSending = false;
   if (!error) { // normal case
       assert(msgs);
       assert(bytes_transferred == bytes_sent);
       if (logger->isTraceEnabled()) {
           for (auto& messageDesc : *msgs) {
               LOG4CXX_TRACE(logger, "handleSendMessage: bytes_transferred="
                             << messageDesc->getMessageSize()
                             << ", "<< getPeerId()
                             << ", " << strMsgType(messageDesc->getMessageType())
                             << ", conn=" << hex << this << dec);
           }
       }

       pushNextMessage();
       return;
   }

   // error case

   assert(error != asio::error::interrupted);
   assert(error != asio::error::would_block);
   assert(error != asio::error::try_again);

   LOG4CXX_ERROR(logger, "Network error #" << error
                 << " (" << error.message() << ") in " << __FUNCTION__
                 << ", peer " << getPeerId());

   for (std::list<std::shared_ptr<MessageDesc> >::const_iterator i = msgs->begin();
        i != msgs->end(); ++i) {
       const std::shared_ptr<MessageDesc>& messageDesc = *i;
       _networkManager.handleConnectionError(_remoteInstanceId, messageDesc->getQueryID(), error);
   }

   if (_connectionState == CONNECTED || _connectionState == AUTH_IN_PROGRESS) {
       disconnectInternal();
   }
   if (_remoteInstanceId == INVALID_INSTANCE) {
       LOG4CXX_TRACE(logger, "Not recovering connection from "<<getPeerId());
       return;
   }

   LOG4CXX_DEBUG(logger, "Recovering connection to " << getPeerId());
   _networkManager.reconnect(_remoteInstanceId);
}

void Connection::pushNextMessage()
{
   // Always use this local copy in this routine.
   bool flushThenDisconnect = _flushThenDisconnect;

   if (_connectionState != CONNECTED && _connectionState != AUTH_IN_PROGRESS) {
      assert(!_isSending);
      LOG4CXX_TRACE(logger, "Not yet connected to " << getPeerId());
      return;
   }
   if (_isSending) {
      LOG4CXX_TRACE(logger, "Already sending to " << getPeerId());
      return;
   }

   vector<asio::const_buffer> constBuffers;
   typedef std::list<std::shared_ptr<MessageDesc> > MessageDescList;
   std::shared_ptr<MessageDescList> msgs = make_shared<MessageDescList>(); //XXX TODO: can be created once
   size_t size(0);
   const size_t maxSize(32*KiB); //XXX TODO: pop all the messages!!!

   // Get sendable messages from the multichannel queue and serialize
   // them into constBuffers.
   while (true) {
       std::shared_ptr<MessageDesc> messageDesc = popMessage();
       if (!messageDesc) {
           break;
       }
       msgs->push_back(messageDesc);
       if (messageDesc->getMessageType() != mtAlive) {
           // mtAlive are useful only if there is no other traffic
           messageDesc->_messageHeader.setSourceInstanceID(
                _selfInstanceId);

           messageDesc->writeConstBuffers(constBuffers);
           size += messageDesc->getMessageSize();
           if (size >= maxSize) {
               break;
           }
       }
   }

   if (msgs->empty()) {
        if(flushThenDisconnect)
        {
            // 1)  A thread can add a message to _messageQueue only if it gets the mutex and
            //     subsequently _flushThenDisconnect=false
            //     See:  pushMessage()
            //
            // 2)  A thread trying to add a message to _messageQueue must thow an exception
            //     if it gets the mutex and subsequently _flushThenDisconnect=true
            //     See:  pushMessage()
            //
            // 3)  A thread can only set _flushThenDisconnect=true after waiting for the mutex
            //     See:  flushThenDisconnect()
            //
            // 4)  When processing queue messages:
            //      A)  If _flushThenDisconnect=false at the beginning of pushNextMessage() then
            //          the thread goes about business as usual
            //
            //      B)  If _flushThenDisconnect=true at the beginning of pushNextMessage() then
            //          we have a guarantee that no new messages can be added to the _messageQueue
            //          other than the messages that will be read in the while loop above (see #2).
            //          The local copy of _flushThenDisconnect thus ensures the equality of msgs
            //          with _messageQueue until the code reaches this disconnect point.
            //
            //      C) If the value of _flushThenDisconnect changes from false to true after the
            //         local copy of flushThenDisconnect is made but prior to the if(msgs->empty()),
            //         then the new message pushed in pushNextMessage() is guaranteed to be
            //         scheduled by the thread that inserted the new message into the _messageQueue.
            //
            disconnectInternal();
        }
        return;
   }

   // Once authentication handshake is done, peer-to-peer messaging
   // can include flow control messages.
   if (_remoteInstanceId != CLIENT_INSTANCE && !isAuthenticating()) {
       std::shared_ptr<MessageDesc> controlMsg = makeControlMessage();
       if (controlMsg) {
           msgs->push_back(controlMsg);
           controlMsg->writeConstBuffers(constBuffers);
           controlMsg->_messageHeader.setSourceInstanceID(
                _selfInstanceId);
           size += controlMsg->getMessageSize();
       }
   }

   if (size == 0) { //XXX TODO: aliveMsg can be created once
       assert(!msgs->empty());
       assert(msgs->front()->getMessageType() == mtAlive);
       std::shared_ptr<MessageDesc>& aliveMsg = msgs->front();
       aliveMsg->writeConstBuffers(constBuffers);
       aliveMsg->_messageHeader.setSourceInstanceID(_selfInstanceId);
       size += aliveMsg->getMessageSize();
   }

   asio::async_write(_socket, constBuffers,
                     bind(&Connection::handleSendMessage,
                          shared_from_this(),
                          asio::placeholders::error,
                          asio::placeholders::bytes_transferred,
                          msgs, size));
   _isSending = true;
}

MessagePtr
Connection::ServerMessageDesc::createRecord(MessageID messageType)
{
    if (isScidbMessage(messageType)) {
      return MessageDesc::createRecord(messageType);
   }

   // Plugin message, consult the factory.
   std::shared_ptr<NetworkMessageFactory> msgFactory;
   msgFactory = getNetworkMessageFactory();
   assert(msgFactory);
   MessagePtr recordPtr = msgFactory->createMessage(messageType);

   if (!recordPtr) {
      LOG4CXX_ERROR(logger, "Unknown message type " << strMsgType(messageType) << " (" << messageType << ')');
      throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_UNKNOWN_MESSAGE_TYPE) << messageType;
   }
   return recordPtr;
}

bool
Connection::ServerMessageDesc::validate()
{
   if (MessageDesc::validate()) {
      return true;
   }
   std::shared_ptr<NetworkMessageFactory> msgFactory;
   msgFactory = getNetworkMessageFactory();
   assert(msgFactory);
   return msgFactory->isRegistered(getMessageType());
}

void Connection::onResolve(std::shared_ptr<asio::ip::tcp::resolver>& resolver,
                           std::shared_ptr<asio::ip::tcp::resolver::query>& query,
                           const boost::system::error_code& err,
                           asio::ip::tcp::resolver::iterator endpoint_iterator)
 {
    assert(query);
    assert(resolver);

    if (_connectionState != CONNECT_IN_PROGRESS ||
        _query != query) {
       LOG4CXX_DEBUG(logger, "Dropping resolve query "
                     << query->host_name() << ":" << query->service_name());
       return;
    }

    asio::ip::tcp::resolver::iterator end;
    if (err || endpoint_iterator == end) {
       _error = err ? err : boost::asio::error::host_not_found;
       if (_logConnectErrors) {
          _logConnectErrors = false;
          LOG4CXX_ERROR(logger, "Network error #"
                        << _error << " (" << _error.message() << ")"
                        << " while resolving name of "
                        << getPeerId() << ", "
                        << _query->host_name() << ":" << _query->service_name());

       }
       disconnectInternal();
       _networkManager.reconnect(_remoteInstanceId);
       return;
    }

    LOG4CXX_TRACE(logger, "Connecting to the first candidate for: "
                  << _query->host_name() << ":" << _query->service_name());
    asio::ip::tcp::endpoint ep = *endpoint_iterator;
    _socket.async_connect(ep,
                          boost::bind(&Connection::onConnect, shared_from_this(),
                                      resolver, query,
                                      ++endpoint_iterator,
                                      boost::asio::placeholders::error));
 }

void Connection::onConnect(std::shared_ptr<asio::ip::tcp::resolver>& resolver,
                           std::shared_ptr<asio::ip::tcp::resolver::query>& query,
                           asio::ip::tcp::resolver::iterator endpoint_iterator,
                           const boost::system::error_code& err)

{
   assert(query);
   assert(resolver);
   asio::ip::tcp::resolver::iterator end;

   if (_connectionState != CONNECT_IN_PROGRESS ||
       _query != query) {
      LOG4CXX_TRACE(logger, "Dropping resolve query "
                    << query->host_name() << ":" << query->service_name());
      return;
   }

   if (err && endpoint_iterator == end) {
      if (_logConnectErrors) {
         _logConnectErrors = false;
         LOG4CXX_ERROR(logger, "Network error #"
                       << err << " (" << err.message() << ")"
                       << " while connecting to "
                       << getPeerId() << ", "
                       << _query->host_name() << ":" << _query->service_name());
      }
      disconnectInternal();
      _error = err;
      _networkManager.reconnect(_remoteInstanceId);
      return;
   }

   if (err) {
      LOG4CXX_TRACE(logger, "Connecting to the next candidate,"
                    << getPeerId() << ", "
                    << _query->host_name() << ":" << _query->service_name()
                    << "Last error #"
                    << err.value() << "('" << err.message() << "')");
      _error = err;
      _socket.close();
      asio::ip::tcp::endpoint ep = *endpoint_iterator;
      _socket.async_connect(ep,
                            boost::bind(&Connection::onConnect,
                                        shared_from_this(),
                                        resolver,
                                        query,
                                        ++endpoint_iterator,
                                        boost::asio::placeholders::error));
      return;
   }

    configConnectedSocket();
    setRemoteIpAndPort();

    LOG4CXX_DEBUG(logger, "Connected to "
                  << getPeerId() << ", "
                  << _query->host_name() << ":"
                  << _query->service_name()
                  << ", conn=" << hex << this << dec);

    // Send instance-to-instance authentication logon and assume that
    // authentication succeeds (if not, peer will disconnect).
    // See SDB-5702.
    _connectionState = AUTH_IN_PROGRESS;
    MessageDescPtr logonMsg = makeLogonMessage();
    sendMessage(logonMsg);
    authDoneInternal();         // Becomes readMessage() if SDB-5702 is solved.
}

void Connection::authDone()
{
    if (::pthread_equal(::pthread_self(), _networkManager.getThreadId())) {
        LOG4CXX_TRACE(logger, "authDone: conn=" << hex << this << dec << ", in netmgr");
        authDoneInternal();
    } else {
        LOG4CXX_TRACE(logger, "authDone: conn=" << hex << this << dec);
        _networkManager.getIOService().post(
            bind(&Connection::authDoneInternal, shared_from_this()));
    }
}

void Connection::authDoneInternal()
{
    if (_connectionState == AUTH_IN_PROGRESS) {
        _connectionState = CONNECTED;

        LOG4CXX_TRACE(logger, "authDoneInternal: conn=" << hex << this << dec
                      << " is now connected");

        _error.clear();
        _query.reset();
        _logConnectErrors = true;

        {
            ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);
            _messageQueue.authDone();
        }

        pushNextMessage();
    } else {
        SCIDB_ASSERT(_connectionState == DISCONNECTED);
        LOG4CXX_TRACE(logger, "authDoneInternal: conn=" << hex << this << dec
                      << " disconnected");
    }
}

void Connection::connectAsync(const string& address, uint16_t port)
{
   _networkManager.getIOService().post(bind(&Connection::connectAsyncInternal,
                                            shared_from_this(), address, port));
}

void Connection::connectAsyncInternal(const string& address, uint16_t port)
{
   if (_connectionState == CONNECTED ||
       _connectionState == CONNECT_IN_PROGRESS) {
      LOG4CXX_WARN(logger, "Already connected/ing! Not Connecting to " << address << ":" << port);
      return;
   }

   disconnectInternal();
   LOG4CXX_TRACE(logger, "Connecting (async) to " << address << ":" << port);

   //XXX TODO: switch to using scidb::resolveAsync()
   std::shared_ptr<asio::ip::tcp::resolver>  resolver(new asio::ip::tcp::resolver(_socket.get_io_service()));
   stringstream serviceName;
   serviceName << port;
   _query.reset(new asio::ip::tcp::resolver::query(address, serviceName.str()));
   _error.clear();
   _connectionState = CONNECT_IN_PROGRESS;
   resolver->async_resolve(*_query,
                           boost::bind(&Connection::onResolve, shared_from_this(),
                                       resolver, _query,
                                       boost::asio::placeholders::error,
                                       boost::asio::placeholders::iterator));
}

void Connection::attachQuery(
    QueryID queryID,
    ClientContext::DisconnectHandler& dh)
{
    // Note:  at this point in time the query object itself has
    // not been instantiated.
    ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);
    _activeClientQueries[queryID] = dh;
}

void Connection::attachQuery(QueryID queryID)
{
    // Note:  at this point in time the query object itself has
    // not been instantiated.
    ClientContext::DisconnectHandler dh;
    attachQuery(queryID, dh);
}

void Connection::detachQuery(QueryID queryID)
{
    ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);
    _activeClientQueries.erase(queryID);
}

void Connection::disconnectInternal()
{
   LOG4CXX_DEBUG(logger, "Disconnecting from " << getPeerId());
   _socket.close();
   _connectionState = DISCONNECTED;
   _query.reset();
   _remoteIp = boost::asio::ip::address();
   _remotePort = 0;
   ClientQueries clientQueries;
   {
       ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);
       clientQueries.swap(_activeClientQueries);
   }

   LOG4CXX_TRACE(logger, str(format("Number of active client queries %lld") % clientQueries.size()));

   for (ClientQueries::const_iterator i = clientQueries.begin();
        i != clientQueries.end(); ++i)
   {
       assert(_remoteInstanceId == CLIENT_INSTANCE);
       QueryID queryID = i->first;
       const ClientContext::DisconnectHandler& dh = i->second;
       _networkManager.handleClientDisconnect(queryID, dh);
   }

   if (_session && _session->hasCleanup()) {
       _networkManager.handleSessionClose(_session);
   }
}

void Connection::disconnect()
{
   _networkManager.getIOService().post(bind(&Connection::disconnectInternal,
                                            shared_from_this()));
}

void Connection::flushThenDisconnect()
{
    ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);
    _flushThenDisconnect = true;
}


void Connection::handleReadError(const boost::system::error_code& error)
{
   assert(error);

   if (error != boost::asio::error::eof) {
      LOG4CXX_ERROR(logger, "Network error while reading, #"
                    << error << " (" << error.message() << ")");
   } else {
      LOG4CXX_TRACE(logger, "Sender disconnected (eof on read)");
   }
   if (_connectionState == CONNECTED || _connectionState == AUTH_IN_PROGRESS) {
       disconnectInternal();
   }
}

Connection::~Connection()
{
   LOG4CXX_TRACE(logger, "Destroying connection to " << getPeerId());
   abortMessages();
   disconnectInternal();
}

void Connection::abortMessages()
{
    MultiChannelQueue connQ(_remoteInstanceId, *this);
    {
        ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);
        connQ.swap(_messageQueue);
    }
    LOG4CXX_TRACE(logger, "Aborting "<< connQ.size()
                  << " buffered connection messages to "
                  << getPeerId());
   connQ.abortMessages();
}

string Connection::getPeerId() const
{
    stringstream ss;
    ss << Iid(_remoteInstanceId);
    if (boost::asio::ip::address() != _remoteIp) {
       boost::system::error_code ec;
       string ip(_remoteIp.to_string(ec));
       assert(!ec);
       ss << " (" << ip << ':' << _remotePort << ')';
    }
    return ss.str();
}

string Connection::getRemoteEndpointName() const
{
    stringstream ss;
    ss << _remoteIp.to_string() << ':' << _remotePort;
    return ss.str();
}

void Connection::setRemoteIpAndPort()
{
   boost::system::error_code ec;
   boost::asio::ip::tcp::endpoint endpoint = _socket.remote_endpoint(ec);
   if (!ec)
   {
      _remoteIp = endpoint.address();
      _remotePort = endpoint.port();
   }
   else
   {
      LOG4CXX_ERROR(logger,
                    "Could not get the remote IP from connected socket to/from "
                    << getPeerId()
                    << ": " << ec.message() << " (" << ec << ")");
   }
}

std::shared_ptr<NetworkManager::ConnectionStatus>
Connection::MultiChannelQueue::_push(NetworkManager::MessageQueueType mqt,
                                     const std::shared_ptr<MessageDesc>& msg,
                                     bool ontoBack)
{
    assert(msg);
    assert(mqt<NetworkManager::mqtMax);

    std::shared_ptr<Channel>& channel = _channels[mqt];
    if (!channel) {
        channel = std::make_shared<Channel>(_instanceId, mqt, _connection);
    }
    bool isActiveBefore = channel->isActive();

    std::shared_ptr<NetworkManager::ConnectionStatus> status;
    status =  ontoBack ? channel->pushBack(msg) : channel->pushUrgent(msg);
    ++_size;

    bool isActiveAfter = channel->isActive();
    if (isActiveBefore != isActiveAfter) {
        (isActiveAfter ? ++_activeChannelCount : --_activeChannelCount);
        assert(_activeChannelCount<=NetworkManager::mqtMax);
    }
    return status;
}

std::shared_ptr<NetworkManager::ConnectionStatus>
Connection::MultiChannelQueue::popFront(std::shared_ptr<MessageDesc>& msg)
{
    assert(!msg);

    Channel *channel(NULL);
    uint32_t start = _currChannel;
    while (true) {
        ++_currChannel;
        channel = _channels[_currChannel % NetworkManager::mqtMax].get();
        if ((_currChannel % NetworkManager::mqtMax) == (start % NetworkManager::mqtMax)) {
            break;
        }
        if (channel == nullptr) {
            continue;
        }
        if (!channel->isActive()) {
            continue;
        }
        break;
    }
    std::shared_ptr<NetworkManager::ConnectionStatus> status;
    if (channel && channel->isActive()) {

        status = channel->popFront(msg);
        assert(msg);
        --_size;

        _activeChannelCount -= (!channel->isActive());
        assert(_activeChannelCount<=NetworkManager::mqtMax);
    }
    return status;
}

std::shared_ptr<MessageDesc> Connection::makeControlMessage()
{
    std::shared_ptr<MessageDesc> msgDesc = make_shared<MessageDesc>(mtControl);
    assert(msgDesc);

    namespace gpb = google::protobuf;

    std::shared_ptr<scidb_msg::Control> record = msgDesc->getRecord<scidb_msg::Control>();
    gpb::uint64 localGenId=0;
    gpb::uint64 remoteGenId=0;
    assert(record);
    {
        ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);

        localGenId = _messageQueue.getLocalGenId();
        record->set_local_gen_id(localGenId);

        remoteGenId = _messageQueue.getRemoteGenId();
        record->set_remote_gen_id(remoteGenId);

        LOG4CXX_TRACE(logger, "Create mtControl message localGenId=" << localGenId
                      <<", remoteGenId=" << remoteGenId);

        // Gather sequence numbers for flow controlled channels.
        // Recall mqtNone is not flow controlled.
        for (uint32_t mqt = (NetworkManager::mqtNone+1); mqt < NetworkManager::mqtMax; ++mqt) {
            const gpb::uint64 localSn   = _messageQueue.getLocalSeqNum(NetworkManager::MessageQueueType(mqt));
            const gpb::uint64 remoteSn  = _messageQueue.getRemoteSeqNum(NetworkManager::MessageQueueType(mqt));
            const gpb::uint32 id        = mqt;
            scidb_msg::Control_Channel* entry = record->add_channels();
            assert(entry);
            entry->set_id(id);
            entry->set_local_sn(localSn);
            entry->set_remote_sn(remoteSn);
        }
    }

    gpb::RepeatedPtrField<scidb_msg::Control_Channel>* entries = record->mutable_channels();
    for(gpb::RepeatedPtrField<scidb_msg::Control_Channel>::iterator iter = entries->begin();
        iter != entries->end(); ++iter) {
        scidb_msg::Control_Channel& entry = (*iter);
        assert(entry.has_id());
        const NetworkManager::MessageQueueType mqt = static_cast<NetworkManager::MessageQueueType>(entry.id());
        const gpb::uint64 available =
            _networkManager.getAvailable(NetworkManager::MessageQueueType(mqt), _remoteInstanceId);
        entry.set_available(available);
    }

    if (logger->isTraceEnabled()) {
        const gpb::RepeatedPtrField<scidb_msg::Control_Channel>& channels = record->channels();
        for(gpb::RepeatedPtrField<scidb_msg::Control_Channel>::const_iterator iter = channels.begin();
            iter != channels.end(); ++iter) {
            const scidb_msg::Control_Channel& entry = (*iter);
            const NetworkManager::MessageQueueType mqt =
                static_cast<NetworkManager::MessageQueueType>(entry.id());
            const uint64_t available    = entry.available();
            const uint64_t remoteSn     = entry.remote_sn();
            const uint64_t localSn      = entry.local_sn();

            LOG4CXX_TRACE(logger, "FlowCtl: SND iid=" << Iid(_remoteInstanceId)
                          << " avail=" << available
                          << " mqt=" << mqt
                          << " lclseq=" << localSn
                          << " rmtseq=" << remoteSn
                          << " lclgen=" << localGenId
                          << " rmtgen=" << remoteGenId);
        }
    }
    return msgDesc;
}

std::shared_ptr<MessageDesc> Connection::makeLogonMessage()
{
    std::shared_ptr<MessageDesc> result = make_shared<MessageDesc>(mtAuthLogon);
    std::shared_ptr<scidb_msg::AuthLogon> record = result->getRecord<scidb_msg::AuthLogon>();
    record->set_username(Iid(_selfInstanceId).str());
    record->set_priority(SessionProperties::NORMAL);
    record->set_authtag(auth::strMethodTag(AUTH_I2I));

    // In the future, something more challenging; for now, just the
    // cluster uuid.
    record->set_signature(Cluster::getInstance()->getUuid());

    return result;
}

std::shared_ptr<NetworkManager::ConnectionStatus>
Connection::MultiChannelQueue::setRemoteState(NetworkManager::MessageQueueType mqt,
                                              uint64_t rSize,
                                              uint64_t localGenId, uint64_t remoteGenId,
                                              uint64_t localSn, uint64_t remoteSn)
{
    // XXX TODO: consider turning asserts into exceptions
    std::shared_ptr<NetworkManager::ConnectionStatus> status;
    if (mqt>=NetworkManager::mqtMax) {
        assert(false);
        return status;
    }
    if (remoteGenId < _remoteGenId) {
        assert(false);
        return status;
    }
    if (localGenId > _localGenId) {
        assert(false);
        return status;
    }
    if (localGenId < _localGenId) {
        localSn = 0;
    }

    std::shared_ptr<Channel>& channel = _channels[mqt];
    if (!channel) {
        channel = std::make_shared<Channel>(_instanceId, mqt, _connection);
    }
    if (!channel->validateRemoteState(rSize, localSn, remoteSn)) {
        assert(false);
        return status;
    }
    if (remoteGenId > _remoteGenId) {
        _remoteGenId = remoteGenId;
    }
    bool isActiveBefore = channel->isActive();

    status = channel->setRemoteState(rSize, localSn, remoteSn);

    bool isActiveAfter = channel->isActive();
    if (isActiveBefore != isActiveAfter) {
        (isActiveAfter ? ++_activeChannelCount : --_activeChannelCount);
        assert(_activeChannelCount<=NetworkManager::mqtMax);
    }
    return status;
}

uint64_t
Connection::MultiChannelQueue::getAvailable(NetworkManager::MessageQueueType mqt) const
{
    assert(mqt<NetworkManager::mqtMax);
    const std::shared_ptr<Channel>& channel = _channels[mqt];
    if (!channel) {
        return NetworkManager::MAX_QUEUE_SIZE;
    }
    return channel->getAvailable();
}

uint64_t
Connection::MultiChannelQueue::getLocalSeqNum(NetworkManager::MessageQueueType mqt) const
{
    assert(mqt<NetworkManager::mqtMax);
    const std::shared_ptr<Channel>& channel = _channels[mqt];
    if (!channel) {
        return 0;
    }
    return channel->getLocalSeqNum();
}

uint64_t
Connection::MultiChannelQueue::getRemoteSeqNum(NetworkManager::MessageQueueType mqt) const
{
    assert(mqt<NetworkManager::mqtMax);
    const std::shared_ptr<Channel>& channel = _channels[mqt];
    if (!channel) {
        return 0;
    }
    return channel->getRemoteSeqNum();
}

void
Connection::MultiChannelQueue::authDone()
{
    size_t numActive = 0;
    for (auto& channel : _channels) {
        if (channel) {
            channel->authDone();
            numActive += channel->isActive();
        }
    }
    _activeChannelCount = numActive;

    SCIDB_ASSERT(bool(numActive) == isActive());
    SCIDB_ASSERT(_activeChannelCount<=NetworkManager::mqtMax);
}

void
Connection::MultiChannelQueue::abortMessages()
{
    for (auto& channel : _channels) {
        if (channel) {
            channel->abortMessages();
        }
    }
    _activeChannelCount = 0;
    _size = 0;
}

void
Connection::MultiChannelQueue::swap(MultiChannelQueue& other)
{
    // No swapping across Connections!
    SCIDB_ASSERT(&_connection == &other._connection);

    const InstanceID instanceId = _instanceId;
    _instanceId = other._instanceId;
    other._instanceId = instanceId;

    const uint32_t currMqt = _currChannel;
    _currChannel = other._currChannel;
    other._currChannel = currMqt;

    const size_t activeCount = _activeChannelCount;
    _activeChannelCount = other._activeChannelCount;
    other._activeChannelCount = activeCount;

    const uint64_t size = _size;
    _size = other._size;
    other._size = size;

    const uint64_t remoteGenId = _remoteGenId;
    _remoteGenId = other._remoteGenId;
    other._remoteGenId = remoteGenId;

    const uint64_t localGenId = _localGenId;
    _localGenId = other._localGenId;
    other._localGenId = localGenId;

    _channels.swap(other._channels);
}

std::shared_ptr<NetworkManager::ConnectionStatus>
Connection::Channel::pushBack(const std::shared_ptr<MessageDesc>& msg)
{
    if ( !_msgQ.empty() &&  msg->getMessageType() == mtAlive) {
        // mtAlive are useful only if there is no other traffic
        assert(_mqt == NetworkManager::mqtNone);
        return  std::shared_ptr<NetworkManager::ConnectionStatus>();
    }
    const uint64_t spaceBefore = getAvailable();
    if (spaceBefore == 0) {
        throw NetworkManager::OverflowException(_mqt, REL_FILE, __FUNCTION__, __LINE__);
    }
    _msgQ.push_back(msg);
    const uint64_t spaceAfter = getAvailable();
    std::shared_ptr<NetworkManager::ConnectionStatus> status = getNewStatus(spaceBefore, spaceAfter);
    return status;
}

std::shared_ptr<NetworkManager::ConnectionStatus>
Connection::Channel::pushUrgent(const std::shared_ptr<MessageDesc>& msg)
{
    // The only messages that get pushed on the front are the
    // (high-priority) authentication messages.
    SCIDB_ASSERT(isAuthMessage(msg->getMessageType()));

    const uint64_t spaceBefore = getAvailable();

    // Push even if no spaceBefore, auth messages disregard _sendQueueLimit.
    // Keep auth messages in FIFO order.
    auto pos = _msgQ.begin();
    while (pos != _msgQ.end() && isAuthMessage((*pos)->getMessageType())) {
        ++pos;
    }
    _msgQ.insert(pos, msg);

    const uint64_t spaceAfter = getAvailable();

    return getNewStatus(spaceBefore, spaceAfter);
}

std::shared_ptr<NetworkManager::ConnectionStatus>
Connection::Channel::popFront(std::shared_ptr<MessageDesc>& msg)
{
    std::shared_ptr<NetworkManager::ConnectionStatus> status;
    if (!isActive()) {
        msg.reset();
        return status;
    }
    const uint64_t spaceBefore = getAvailable();
    msg = _msgQ.front();
    _msgQ.pop_front();
    ++_localSeqNum;
    const uint64_t spaceAfter = getAvailable();
    status = getNewStatus(spaceBefore, spaceAfter);

    LOG4CXX_TRACE(logger, "popFront: Channel "<< _mqt
                  << " to " << Iid(_instanceId) << " "
                  << ((isActive()) ? "ACTIVE" : "BLOCKED"));

    return status;
}

std::shared_ptr<NetworkManager::ConnectionStatus>
Connection::Channel::setRemoteState(uint64_t rSize, uint64_t localSn, uint64_t remoteSn)
{
   const uint64_t spaceBefore = getAvailable();
    _remoteSize = rSize;
    _remoteSeqNum = remoteSn;
    _localSeqNumOnPeer = localSn;
    const uint64_t spaceAfter = getAvailable();
    std::shared_ptr<NetworkManager::ConnectionStatus> status = getNewStatus(spaceBefore, spaceAfter);

    LOG4CXX_TRACE(logger, "setRemoteState: Channel "<< _mqt
                  << " to " << Iid(_instanceId)
                  << ", remoteSize="<<_remoteSize
                  << ", remoteSeqNum="<<_remoteSeqNum
                  << ", remoteSeqNumOnPeer="<<_localSeqNumOnPeer);
    return status;
}

void
Connection::Channel::abortMessages()
{
    MessageQueue mQ;
    mQ.swap(_msgQ);
    LOG4CXX_TRACE(logger, "abortMessages: Aborting "<< mQ.size()
                  << " buffered connection messages to "
                  << Iid(_instanceId));
    std::set<QueryID> queries;
    for (MessageQueue::iterator iter = mQ.begin();
         iter != mQ.end(); ++iter) {
        std::shared_ptr<MessageDesc>& messageDesc = (*iter);
        queries.insert(messageDesc->getQueryID());
    }
    mQ.clear();
    NetworkManager* networkManager = NetworkManager::getInstance();
    assert(networkManager);
    boost::system::error_code aborted = boost::asio::error::operation_aborted;
    networkManager->handleConnectionError(_instanceId, queries, aborted);
}

std::shared_ptr<NetworkManager::ConnectionStatus>
Connection::Channel::getNewStatus(const uint64_t spaceBefore,
                                  const uint64_t spaceAfter)
{
    if ((spaceBefore != spaceAfter) && (spaceBefore == 0 || spaceAfter == 0)) {
        return std::make_shared<NetworkManager::ConnectionStatus>(_instanceId, _mqt, spaceAfter);
    }
    return std::shared_ptr<NetworkManager::ConnectionStatus>();
}

uint64_t
Connection::Channel::getAvailable() const
{
    const uint64_t localLimit = _sendQueueLimit;
    const uint64_t localSize  = _msgQ.size();

    if (localSize >= localLimit) {
        return 0;
    }
    return localLimit - localSize;
}

void
Connection::setAuthenticator(std::shared_ptr<Authenticator>& author)
{
    ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);
    ASSERT_EXCEPTION(!_session, "Connection already authenticated?!");
    if (_authenticator) {
        throw USER_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_AUTHENTICATION_ERROR)
            << "Multiple logins on one connection";
    }
    _authenticator = author;
}

void
Connection::setSession(std::shared_ptr<Session>& sess)
{
    ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);
    ASSERT_EXCEPTION(_authenticator, "No authenticator, so who's calling me?");
    ASSERT_EXCEPTION(!_session, "Already authenticated?!");
    _authenticator.reset();
    _session = sess;
}

} // namespace
