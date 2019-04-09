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
 * @file NetworkManager.cpp
 * @author roman.somakov@gmail.com
 *
 * @brief NetworkManager class implementation.
 */
#include "NetworkManager.h"

#include <sys/types.h>
#include <boost/bind.hpp>
#include <boost/format.hpp>
#include <memory>
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>

#include <monitor/InstanceStats.h>
#include <monitor/MonitorConfig.h>
#include <network/MessageHandleJob.h>
#include <network/MessageUtils.h>
#include <network/AuthMessageHandleJob.h>
#include <network/ClientMessageHandleJob.h>
#include <network/Connection.h>
#include <network/MessageUtils.h>
#include <network/OrderedBcast.h>
#include <network/ThrottledScheduler.h>
#include <array/Metadata.h>
#include <array/MemoryBuffer.h>
#include <array/SharedBuffer.h>
#include <query/Query.h>
#include <storage/StorageMgr.h>
#include <system/Config.h>
#include <system/Constants.h>
#include <system/Exceptions.h>
#include <system/SystemCatalog.h>
#include <system/Utils.h>
#include <rbac/Session.h>
#include <rbac/SessionProperties.h>
#include <util/Notification.h>
#include <util/PluginManager.h>

using namespace std;
using namespace boost;

namespace scidb
{

// Logger for network subsystem. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network"));

/***
 * N e t w o r k M a n a g e r
 */
volatile bool NetworkManager::_shutdown=false;

NetworkManager::NetworkManager()
    :   InjectedErrorListener(InjectErrCode::FINAL_BROADCAST),
        _acceptor(_ioService,
                boost::asio::ip::tcp::endpoint(
                    boost::asio::ip::tcp::v4(),
                    safe_static_cast<uint16_t>(
                        Config::getInstance()->getOption<int>(CONFIG_PORT)))),
        _input(_ioService),
        _aliveTimer(_ioService),
        _aliveTimeout(DEFAULT_ALIVE_TIMEOUT_MICRO),
        _selfInstanceID(INVALID_INSTANCE),
        _selfThreadID((pthread_t)0),
        _currConnGenId(0),
        _repMessageCount(0),
        _maxRepSendQSize(Config::getInstance()->getOption<int>(CONFIG_REPLICATION_SEND_QUEUE_SIZE)),
        _maxRepReceiveQSize(Config::getInstance()->getOption<int>(CONFIG_REPLICATION_RECEIVE_QUEUE_SIZE)),
        _randInstanceIndx(0),
        _aliveRequestCount(0),
        _memUsage(0),
        _msgHandlerFactory(new DefaultNetworkMessageFactory),
        _ipAddress(""),
        _livenessSubscriberID(0)
{
    // Note: that _acceptor is 'fully opened', i.e. bind()'d, listen()'d and polled as needed
    _acceptor.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));

    Scheduler::Work func = bind(&NetworkManager::handleLiveness);
    _livenessHandleScheduler =
       std::shared_ptr<ThrottledScheduler>(new ThrottledScheduler(DEFAULT_LIVENESS_HANDLE_TIMEOUT,
                                                             func, _ioService));
    LOG4CXX_DEBUG(logger, "Network manager is initialized");
}

NetworkManager::~NetworkManager()
{
    LOG4CXX_DEBUG(logger, "Network manager is shutting down");
    _ioService.stop();
    if (_livenessSubscriberID) {
        Notification<InstanceLiveness>::unsubscribe(_livenessSubscriberID);
    }
}

void NetworkManager::run(std::shared_ptr<JobQueue> jobQueue)
{
    LOG4CXX_DEBUG(logger, "NetworkManager::run()"); // called from entry.cpp:292 (runSciDB)
    _selfThreadID = ::pthread_self();

    Config *cfg = Config::getInstance();
    assert(cfg);

    if (cfg->getOption<int>(CONFIG_PORT) == 0) {
        LOG4CXX_WARN(logger, "NetworkManager::run(): Starting to listen on an arbitrary port! (--port=0)");
    }
    boost::asio::ip::tcp::endpoint endPoint = _acceptor.local_endpoint();
    const string address = cfg->getOption<string>(CONFIG_INTERFACE);
    const uint16_t port = endPoint.port();
    _ipAddress = endPoint.address().to_string();  // Save the value for resource monitoring

    const bool registerInstance = cfg->getOption<bool>(CONFIG_REGISTER);

    SystemCatalog* catalog = SystemCatalog::getInstance();
    const string& storageConfigPath = cfg->getOption<string>(CONFIG_STORAGE);

    StorageMgr::getInstance()->openStorage(storageConfigPath);
    _selfInstanceID = StorageMgr::getInstance()->getInstanceId();

    if (registerInstance) {
        if (_selfInstanceID != INVALID_INSTANCE) {
            throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_ALREADY_REGISTERED)
                << Iid(_selfInstanceID);
        }

        // storageConfigDir should be of the form <base-dir>/<server-id>/<server-instance-id>
        string storageConfigDir = scidb::getDir(storageConfigPath);
        if (!isFullyQualified(storageConfigDir)) {
            throw (USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NON_FQ_PATH_ERROR) << storageConfigPath);
        }

        stringstream ss;
        uint32_t sid(~0);
        uint32_t siid(~0);

        string serverInstanceId = scidb::getFile(storageConfigDir);
        ss.str(serverInstanceId);
        if (serverInstanceId.empty() || !(ss >> siid) || !ss.eof()) {
            throw (USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_INSTANCE_PATH_FORMAT_ERROR) << storageConfigPath);
        }

        string serverInstanceIdDir = scidb::getDir(storageConfigDir);
        if (serverInstanceIdDir.empty() || serverInstanceIdDir=="." || serverInstanceIdDir=="/") {
            throw (USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_INSTANCE_PATH_FORMAT_ERROR) << storageConfigPath);
        }
        string serverId = scidb::getFile(serverInstanceIdDir);
        ss.clear();
        ss.str(serverId);
        if (serverId.empty() || !(ss >> sid) || !ss.eof()) {
            throw (USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_INSTANCE_PATH_FORMAT_ERROR) << storageConfigPath);
        }

        string basePath = scidb::getDir(serverInstanceIdDir);
        if (serverInstanceIdDir.empty() || serverInstanceIdDir=="." || serverInstanceIdDir=="/") {
            throw (USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_INSTANCE_PATH_FORMAT_ERROR) << storageConfigPath);
        }

        LOG4CXX_DEBUG(logger, "server-id = " << sid);
        LOG4CXX_DEBUG(logger, "server-instance-id = " << siid);

        const string online = cfg->getOption<string>(CONFIG_ONLINE);
        _selfInstanceID = catalog->addInstance(InstanceDesc(address, port, basePath, sid, siid), online);

        StorageMgr::getInstance()->setInstanceId(_selfInstanceID);
        LOG4CXX_DEBUG(logger, "Registered instance # " << Iid(_selfInstanceID));
        return;
    } else {
        if (_selfInstanceID == INVALID_INSTANCE) {
            throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_NOT_REGISTERED);
        }
        fetchInstances();

        ServerCounter sc;
        _instanceMembership->visitInstances(sc);
        const size_t serverCount = sc.getCount();
        const size_t redundancy = cfg->getOption<size_t>(CONFIG_REDUNDANCY);
        if (redundancy >= serverCount) {
            throw USER_EXCEPTION(SCIDB_SE_CONFIG, SCIDB_LE_INVALID_REDUNDANCY)
                    << redundancy << serverCount << MAX_REDUNDANCY;
        }
    }
    _jobQueue = jobQueue;

    // make sure we have at least one thread in the client request queue
    const uint32_t nJobs = std::max(cfg->getOption<int>(CONFIG_EXECUTION_THREADS), 3);

    uint32_t adminJobs = std::max(cfg->getOption<int>(CONFIG_ADMIN_QUERIES), 1);
    adminJobs = std::min(nJobs-2, adminJobs);

    uint32_t clientJobs = std::max(cfg->getOption<int>(CONFIG_CLIENT_QUERIES), 0);
    if (clientJobs == 0) {
        // ignore this config
        clientJobs = nJobs-adminJobs-1;
    }
    clientJobs = std::min(clientJobs, nJobs-adminJobs-1);

    const uint32_t nRequests = std::max(cfg->getOption<int>(CONFIG_REQUESTS),1);

    SCIDB_ASSERT(clientJobs > 0);
    SCIDB_ASSERT(adminJobs > 0);
    SCIDB_ASSERT(clientJobs + adminJobs < nJobs);

    _requestQueue = std::make_shared<WorkQueue>(jobQueue, "NetworkManagerRequestWorkQueue", clientJobs, nRequests);

    _adminRequestQueue = std::make_shared<WorkQueue>(jobQueue, "NetworkManagerAdminRequestWorkQueue", adminJobs, adminJobs*2);

    _workQueue = std::make_shared<WorkQueue>(jobQueue, "NetworkManagerWorkQueue", nJobs - 1);

    LOG4CXX_INFO(logger, "Network manager is started on "
                 << address << ":" << port << " instance #" << Iid(_selfInstanceID));

    if (!cfg->getOption<bool>(CONFIG_NO_WATCHDOG)) {
       startInputWatcher();
    }

    Notification<InstanceLiveness>::Subscriber listener = bind(&handleLivenessNotification, _1);
    _livenessSubscriberID = Notification<InstanceLiveness>::subscribe(listener);

    OrderedBcastManager::getInstance()->init();

    startAccept();

    _aliveTimer.expires_from_now(posix_time::microseconds(0));  //i.e. immediately
    _aliveTimer.async_wait(NetworkManager::handleAlive);

    LOG4CXX_DEBUG(logger, "Start connection accepting and async message exchanging");

    InjectedErrorListener::start();

    // main loop
    _ioService.run();

    InjectedErrorListener::stop();
}

void NetworkManager::handleShutdown()
{
   LOG4CXX_INFO(logger, "SciDB is going down ...");
   ConnectionMap outConns;
   {
       ScopedMutexLock scope(_mutex, PTW_SML_NM);
       assert(_shutdown);
       boost::system::error_code ec;
       _acceptor.close(ec); //ignore error
       _input.close(ec); //ignore error
       outConns.swap(_outConnections);
       getIOService().stop();
   }
}

void NetworkManager::startInputWatcher()
{
   _input.assign(STDIN_FILENO);
   _input.async_read_some(boost::asio::buffer((void*)&one_byte_buffer,sizeof(one_byte_buffer)),
                          bind(&NetworkManager::handleInput, this,
                               boost::asio::placeholders::error,
                               boost::asio::placeholders::bytes_transferred));
}

void NetworkManager::handleInput(const boost::system::error_code& error, size_t bytes_transferr)
{
    boost::system::error_code ec;
    _input.close(ec); //ignoring error

   if (error == boost::system::errc::operation_canceled) {
      return;
   }
   if (!error) {
      LOG4CXX_INFO(logger, "Got std input event. Terminating myself.");
      // Send SIGTERM to ourselves
      // to initiate the normal shutdown process
      assert(one_byte_buffer == 1);
      kill(getpid(), SIGTERM);
   } else {
      LOG4CXX_INFO(logger, "Got std input error: "
                   << error.message() << " (" << error << ')'
                   << ". Killing myself.");
      // let us die
      kill(getpid(), SIGKILL);
   }
}

void NetworkManager::startAccept()
{
   assert(_selfInstanceID != INVALID_INSTANCE);
   std::shared_ptr<Connection> newConnection =
           std::make_shared<Connection>(*this, getNextConnGenId(), _selfInstanceID);
   _acceptor.async_accept(newConnection->getSocket(),
                          boost::bind(&NetworkManager::handleAccept, this,
                                      newConnection, boost::asio::placeholders::error));
}

void NetworkManager::handleAccept(std::shared_ptr<Connection>& newConnection,
                                  const boost::system::error_code& error)
{
    if (error == boost::system::errc::operation_canceled) {
        return;
    }

    if (false) {
        // XXX TODO: we need to provide bookkeeping to limit the number of client connection
        LOG4CXX_DEBUG(logger, "Connection dropped: too many connections");
        return;
    }
    if (!error)
    {
        // XXX TODO: we need to provide bookkeeping to reap stale incoming connections
        LOG4CXX_DEBUG(logger, "Accepted connection " << hex << newConnection.get() << dec);
        newConnection->start();
        startAccept();
    }
    else
    {
        LOG4CXX_ERROR(logger, "Error #" << error
                      << " : '" << error.message()
                      << "' when accepting connection");
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_CANT_ACCEPT_CONNECTION)
              << error << error.message();
    }
}

void NetworkManager::dispatchMessage(const std::shared_ptr<Connection>& connection,
                                     const std::shared_ptr<MessageDesc>& messageDesc)
{
    LOG4CXX_TRACE(logger, "NetworkManager::"<<__func__<<" start");

    SCIDB_ASSERT(messageDesc);
    try
    {
        NetworkMessageFactory::MessageHandler handler;

        bool fromClient = false;
        std::shared_ptr<Session> sess;
        if (connection) {
            sess = connection->getSession();
            if (sess) {
                fromClient = sess->remoteIsClient();
            }
        } else {
            // No connection implies sendLocal(), so we can trust the
            // message's source instance id.
            fromClient = (messageDesc->getSourceInstanceID() == CLIENT_INSTANCE);
        }

        if (connection && !sess) {
            // Connection has no Session object yet, so client must
            // still be authenticating.  Authentication is *required*
            // before any other kind of dispatch is possible.
            std::shared_ptr<MessageHandleJob> job =
                std::make_shared<AuthMessageHandleJob>(connection, messageDesc);
            job->dispatch(this);
            handler = bind(&NetworkManager::publishMessage, _1);
        }
        else if (!handleNonSystemMessage(messageDesc, handler)) {
            assert(!handler);

            if (fromClient)
            {
                ASSERT_EXCEPTION(messageDesc->getSourceInstanceID() == CLIENT_INSTANCE,
                                 "Client pretending to be instance");
                std::shared_ptr<ClientMessageHandleJob> job =
                        std::make_shared<ClientMessageHandleJob>(connection, messageDesc);
                job->dispatch(this);
            }
            else
            {
                ASSERT_EXCEPTION(messageDesc->getSourceInstanceID() != CLIENT_INSTANCE,
                                 "Instance failed to provide source id");
                std::shared_ptr<MessageHandleJob> job =
                        std::make_shared<ServerMessageHandleJob>(messageDesc);
                LOG4CXX_TRACE(logger, "NetworkManager::"<<__func__<<" created a ServerMessageHandleJob(message)");
                job->dispatch(this);
                LOG4CXX_TRACE(logger, "NetworkManager::"<<__func__<<" dispached it");
            }
            handler = bind(&NetworkManager::publishMessage, _1);
        }
        if (handler) {
            LOG4CXX_TRACE(logger, "NetworkManager::"<<__func__<<" calling dispatchMessageToListener)");
            dispatchMessageToListener(connection, messageDesc, handler);
        }
    }
    catch (const Exception& e)
    {
        // It's possible to continue message handling for other queries so we just log an error message.
        //XXX memory & overflow errors need to be considered especially for local messages
        InstanceID instanceId = messageDesc->getSourceInstanceID();
        MessageType messageType = static_cast<MessageType>(messageDesc->getMessageType());
        QueryID queryId = messageDesc->getQueryID();

        LOG4CXX_ERROR(logger, "Exception in message handler: " << e.what());
        LOG4CXX_ERROR(logger, "Exception in message handler: messageType = "<< strMsgType(messageType));
        LOG4CXX_ERROR(logger, "Exception in message handler: source instance ID = " << Iid(instanceId));

        if (messageType != mtError
            && messageType != mtCancelQuery
            && messageType != mtAbort
            && queryId.isValid()
            && instanceId != INVALID_INSTANCE
            && instanceId != _selfInstanceID
            && instanceId != CLIENT_INSTANCE)
        {
            std::shared_ptr<MessageDesc> errorMessage = makeErrorMessageFromException(e, queryId);
            _sendPhysical(instanceId, errorMessage); // if possible
            LOG4CXX_DEBUG(logger, "Error returned to sender");
        }
    }
    LOG4CXX_TRACE(logger, "NetworkManager::"<<__func__<<" ****end");
}

void NetworkManager::handleMessage(std::shared_ptr< Connection >& connection,
                                   const std::shared_ptr<MessageDesc>& messageDesc)
{
   if (_shutdown) {
      handleShutdown();
      return;
   }

   SCIDB_ASSERT(!_mutex.isLockedByThisThread());
   SCIDB_ASSERT(messageDesc);

   if (messageDesc->getMessageType() == mtAlive) {
       return;
   }

   InstanceID instanceId = messageDesc->getSourceInstanceID();

   // During authentication we'll temporarily be receiving messages on
   // what is otherwise an outbound connection, hence the test for
   // isInbound() here.  The authentication handshake itself is
   // responsible for ensuring the ordering of auth messages.

   if (isValidPhysicalInstance(instanceId) && connection->isInbound()) {

       // This is how we enforce instance-to-instance FIFO ordering of messages
       // It can still be lossy, but a single TCP connection at a time should
       // guarantee FIFO ordering.

       const uint64_t connGenId = connection->getGenId();
       std::pair<ConnectionGenMap::iterator,bool> res =
               _inConnections.insert(std::make_pair(instanceId, connGenId));

       if (!res.second) {
           if (res.first->second == connGenId) {
             // normal case
           } else if (res.first->second < connGenId) {
               // record the new connection
               res.first->second = connGenId;
               // report the problem
               bool dummy(false);
               if (!isDead(instanceId, dummy)) {
                   handleConnectionErrorToLive(instanceId);
               }
           } else {
               // ignore the old connection
               LOG4CXX_WARN(logger,
                            "NOT dispatching message from an old connection from instanceID="
                            << Iid(instanceId) << " messageType=" << strMsgType(messageDesc->getMessageType()));
               connection->disconnect();
               return;
           }
       }
   }

   if(MonitorConfig::getInstance()->isEnabled())
   {
       InstanceStats::getInstance()->addToNetRecv(messageDesc->getMessageSize());

       std::shared_ptr<scidb::Query> query = Query::getQueryByID(messageDesc->getQueryID(), false);
       if(query) {
            query->getStats().addToNetRecv(messageDesc->getMessageSize());
       }
   }

   if (messageDesc->getMessageType() == mtControl) {
       handleControlMessage(messageDesc);
       return;
   }

   dispatchMessage(connection, messageDesc);
}

void NetworkManager::handleControlMessage(const std::shared_ptr<MessageDesc>& msgDesc)
{
    assert(msgDesc);
    std::shared_ptr<scidb_msg::Control> record = msgDesc->getRecord<scidb_msg::Control>();
    assert(record);

    InstanceID instanceId = msgDesc->getSourceInstanceID();
    if (instanceId == CLIENT_INSTANCE) {
        return;
    }
    //XXX TODO: convert assert()s to connection->close()
    if(!record->has_local_gen_id()) {
        assert(false);
        return;
    }
    if(!record->has_remote_gen_id()) {
        assert(false);
        return;
    }

    const google::protobuf::RepeatedPtrField<scidb_msg::Control_Channel>& entries = record->channels();
    for(  google::protobuf::RepeatedPtrField<scidb_msg::Control_Channel>::const_iterator iter = entries.begin();
          iter != entries.end(); ++iter) {

        const scidb_msg::Control_Channel& entry = (*iter);
        if(!entry.has_id()) {
            assert(false);
            return;
        }
        if(!entry.has_available()) {
            assert(false);
            return;
        }
        if(!entry.has_local_sn()) {
            assert(false);
            return;
        }
        if(!entry.has_remote_sn()) {
            assert(false);
            return;
        }
        MessageQueueType mqt = static_cast<MessageQueueType>(entry.id());
        if (mqt < mqtNone || mqt >= mqtMax) {
            assert(false);
            return;
        }
    }

    ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);

    ConnectionMap::iterator cIter = _outConnections.find(instanceId);
    if (cIter == _outConnections.end()) {
        return;
    }
    std::shared_ptr<Connection>& connection = cIter->second;
    if (!connection) {
        return;
    }

    uint64_t peerLocalGenId = record->local_gen_id();
    uint64_t peerRemoteGenId = record->remote_gen_id();
    for(google::protobuf::RepeatedPtrField<scidb_msg::Control_Channel>::const_iterator iter = entries.begin();
        iter != entries.end(); ++iter) {

        const scidb_msg::Control_Channel& entry = (*iter);
        const MessageQueueType mqt  = static_cast<MessageQueueType>(entry.id());
        const uint64_t available    = entry.available();
        const uint64_t peerRemoteSn = entry.remote_sn(); //my last SN seen by peer
        const uint64_t peerLocalSn  = entry.local_sn();  //last SN sent by peer to me

        LOG4CXX_TRACE(logger, "FlowCtl: RCV iid=" << Iid(instanceId)
                      << " avail=" << available
                      << " mqt=" << mqt
                      << " peer: ( lclseq=" << peerLocalSn
                      << " rmtseq=" << peerRemoteSn
                      << " lclgen=" << peerLocalGenId
                      << " rmtgen=" << peerRemoteGenId
                      << " )");

        connection->setRemoteQueueState(mqt, available,
                                        peerRemoteGenId, peerLocalGenId,
                                        peerRemoteSn, peerLocalSn);
    }
}

std::shared_ptr<WorkQueue> NetworkManager::getRequestQueue(int p)
{
    ASSERT_EXCEPTION(SessionProperties::validPriority(p),
                     "Unexpected session priority");

    if (p == SessionProperties::ADMIN) {
        return _adminRequestQueue;
    } else {
        return _requestQueue;
    }
}

uint64_t NetworkManager::getAvailable(MessageQueueType mqt, InstanceID forInstanceID)
{
    // mqtRplication is the only supported type for now
    if (mqt != mqtReplication) {
        assert(mqt==mqtNone);
        return MAX_QUEUE_SIZE;
    }
    ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);
    return _getAvailable(mqt, forInstanceID);
}

uint64_t NetworkManager::getAvailableRepSlots()
{
    SCIDB_ASSERT(_mutex.isLockedByThisThread());

    const uint64_t messageCount = _repMessageCount;
    const uint64_t maxReceiveQSize = _maxRepReceiveQSize;

    uint64_t softLimit = 3*maxReceiveQSize/4;
    if (softLimit==0) {
        softLimit=1;
    }

    uint64_t availableSlots = 0;
    if (softLimit > messageCount) {
        availableSlots = (softLimit - messageCount);
    }
    return availableSlots;
}

bool NetworkManager::isBufferSpaceLow()
{
    SCIDB_ASSERT(_mutex.isLockedByThisThread());
    fetchInstances();
    const uint64_t numInst = _instanceMembership->getNumInstances();
    const uint64_t availableSlots = getAvailableRepSlots();
    return (availableSlots < numInst);
}

uint64_t NetworkManager::_getAvailable(MessageQueueType mqt, InstanceID forInstanceID)
{
    SCIDB_ASSERT(_mutex.isLockedByThisThread());

    assert(mqt==mqtReplication);

    fetchInstances();
    const uint64_t numInst = _instanceMembership->getNumInstances();

    const uint64_t availableSlots = getAvailableRepSlots();
    uint64_t available = 0;

    if (availableSlots>0) {

        available = availableSlots / numInst;

        if (available == 0) {
            // There is some space for the incoming chunks,
            // but not enough to accomodate one from each instance.
            // Since we dont know who is going to send to us, we choose a random instance.
            // The instances around it (provided the space allows) get a green light.
            // The same random instance remains the number of instances requests because
            // a control message typically is broadcast resulting in #instances requests at once.
            // Empirically, it seems to work as expected. If an overflow does occur, the query will abort.
            if ((_aliveRequestCount++ % numInst) == 0) {
                _randInstanceIndx  = uint64_t(Query::getRandom()) % numInst;
            }
            uint64_t forInstanceIndx(0);
            try {
                forInstanceIndx = _instanceMembership->getIndex(forInstanceID);
            } catch (scidb::InstanceMembership::NotFoundException& e) {
                LOG4CXX_WARN(logger, "Available queue size=" << available
                             << " for queue "<< mqt
                             << " for non-existent instanceID=" << Iid(forInstanceID));
                return available;
            }
            const uint64_t distLeft  = _randInstanceIndx > forInstanceIndx ?
                                       _randInstanceIndx - forInstanceIndx : forInstanceIndx - _randInstanceIndx;
            const uint64_t distRight = numInst - distLeft;
            if ( distLeft <= availableSlots/2 ||
                 distRight < availableSlots/2 ) {
                available = 1;
            }
        }
    }
    LOG4CXX_TRACE(logger, "Available queue size=" << available
                  << " for queue "<< mqt
                  << " for instanceID=" << Iid(forInstanceID));
    return available;
}

void NetworkManager::registerMessage(const std::shared_ptr<MessageDesc>& messageDesc,
                                     MessageQueueType mqt)
{
    ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);

    _memUsage += messageDesc->getMessageSize();

    LOG4CXX_TRACE(logger, "NetworkManager::registerMessage _memUsage=" << _memUsage);

    // mqtReplication is the only supported type for now
    if (mqt != mqtReplication) {
        assert(mqt == mqtNone);
        return;
    }

    ++_repMessageCount;

    LOG4CXX_TRACE(logger, "Registered message " << _repMessageCount
                  << " for queue "<<mqt << " aliveTimeout="<<_aliveTimeout);

    scheduleAliveNoLater(CONTROL_MSG_TIMEOUT_MICRO);
}

void NetworkManager::unregisterMessage(const std::shared_ptr<MessageDesc>& messageDesc,
                                       MessageQueueType mqt)
{
    ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);

    assert(_memUsage>= messageDesc->getMessageSize());

    _memUsage -= messageDesc->getMessageSize();

    LOG4CXX_TRACE(logger, "NetworkManager::unregisterMessage _memUsage=" << _memUsage);

    // mqtRplication is the only supported type for now
    if (mqt != mqtReplication) {
        assert(mqt == mqtNone);
        return;
    }

    --_repMessageCount;
    LOG4CXX_TRACE(logger, "Unregistered message " << _repMessageCount+1
                  << " for queue "<<mqt  << " aliveTimeout="<<_aliveTimeout);

    scheduleAliveNoLater(CONTROL_MSG_TIMEOUT_MICRO);
}

/// internal
uint64_t
NetworkManager::getSendQueueLimit(MessageQueueType mqt)
{
    // mqtRplication is the only supported type for now
    if (mqt == mqtReplication) {
        ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);
        fetchInstances();
        SCIDB_ASSERT(_instanceMembership->getNumInstances()>0);
        return (_maxRepSendQSize / _instanceMembership->getNumInstances());
    }
    SCIDB_ASSERT(mqt==mqtNone);
    return MAX_QUEUE_SIZE;
}

uint64_t
NetworkManager::getReceiveQueueHint(MessageQueueType mqt)
{
    // mqtRplication is the only supported type for now
    if (mqt == mqtReplication) {
        ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);
        fetchInstances();
        SCIDB_ASSERT(_instanceMembership->getNumInstances()>0);
        return (_maxRepReceiveQSize / _instanceMembership->getNumInstances());
    }
    SCIDB_ASSERT(mqt==mqtNone);
    return MAX_QUEUE_SIZE;
}

void
NetworkManager::scheduleAliveNoLater(const time_t timeoutMicro)
{
    if ( _aliveTimeout > timeoutMicro) {
        _aliveTimeout = timeoutMicro;
        getIOService().post(boost::bind(&NetworkManager::handleAlive,
                                        boost::system::error_code()));
    }
}

bool
NetworkManager::handleNonSystemMessage(const std::shared_ptr<MessageDesc>& messageDesc,
                                       NetworkMessageFactory::MessageHandler& handler)
{
   assert(messageDesc);
   MessageID msgID = messageDesc->getMessageType();
   if (!isPluginMessage(msgID)) {
      return false;
   }
   handler = _msgHandlerFactory->getMessageHandler(msgID);
   if (handler.empty()) {
      LOG4CXX_WARN(logger, "Registered message handler (MsgID="<< msgID <<") is empty!");
      return true;
   }
   return true;
}

void NetworkManager::publishMessage(const std::shared_ptr<MessageDescription>& msgDesc)
{
   LOG4CXX_TRACE(logger, "NetworkManager::"<<__func__<<" begin");
   std::shared_ptr<const MessageDescription> msg(msgDesc);
   Notification<MessageDescription> event(msg);
   event.publish();
}

void NetworkManager::dispatchMessageToListener(const std::shared_ptr<Connection>& connection,
                                               const std::shared_ptr<MessageDesc>& messageDesc,
                                               NetworkMessageFactory::MessageHandler& handler)
{
    // no locks must be held
    SCIDB_ASSERT(!_mutex.isLockedByThisThread());

    std::shared_ptr<MessageDescription> msgDesc;

    if (messageDesc->getSourceInstanceID() == CLIENT_INSTANCE) {
        msgDesc = std::shared_ptr<MessageDescription>(
            new DefaultMessageDescription(connection,
                                          messageDesc->getMessageType(),
                                          messageDesc->getRecord<Message>(),
                                          messageDesc->getBinary(),
                                          messageDesc->getQueryID()
                                          ));
    } else {
        msgDesc = std::shared_ptr<MessageDescription>(
            new DefaultMessageDescription(messageDesc->getSourceInstanceID(),
                                          messageDesc->getMessageType(),
                                          messageDesc->getRecord<Message>(),
                                          messageDesc->getBinary(),
                                          messageDesc->getQueryID()
                                          ));
    }
    // invoke in-line, the handler is not expected to block
    LOG4CXX_TRACE(logger, "NetworkManager::"<<__func__<<" calling handler(msgDesc)");
    handler(msgDesc);
}

void
NetworkManager::_sendPhysical(InstanceID targetInstanceID,
                              std::shared_ptr<MessageDesc>& messageDesc,
                              MessageQueueType mqt /* = mqtNone */)
{
    SCIDB_ASSERT(isValidPhysicalInstance(targetInstanceID));
    if (_shutdown) {
        handleShutdown();
        boost::system::error_code aborted = boost::asio::error::operation_aborted;
        handleConnectionError(targetInstanceID, messageDesc->getQueryID(), aborted);
        return;
    }
    ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);

    assert(_selfInstanceID != INVALID_INSTANCE);
    assert(targetInstanceID != _selfInstanceID);

    // Opening connection if it's not opened yet
    std::shared_ptr<Connection> connection = _outConnections[targetInstanceID];
    if (!connection)
    {
        fetchInstances();
        try {
            const InstanceDesc& instanceDesc = _instanceMembership->getConfig(targetInstanceID);
            const uint64_t genId(0);
            connection = std::make_shared<Connection>(*this, genId, _selfInstanceID, targetInstanceID);
            _outConnections[targetInstanceID] = connection;
            connection->connectAsync(instanceDesc.getHost(), instanceDesc.getPort());
        } catch (const scidb::InstanceMembership::NotFoundException& e) {
            if (isDebug()) {
                std::shared_ptr<Query> query(Query::getQueryByID(messageDesc->getQueryID(), false));
                if (query) {
                    SCIDB_ASSERT(_instanceMembership->getId() >
                                 query->getCoordinatorLiveness()->getMembershipId());
                }
            }
            handleConnectionError(targetInstanceID, messageDesc->getQueryID(), e);
            return;
        }
    }

    // Sending message through connection
    connection->sendMessage(messageDesc, mqt);

    if (mqt == mqtReplication) {
        scheduleAliveNoLater(CONTROL_MSG_TIMEOUT_MICRO);
    }
}

void
NetworkManager::sendPhysical(InstanceID targetInstanceID,
                            std::shared_ptr<MessageDesc>& messageDesc,
                            MessageQueueType mqt)
{
    fetchInstances();
    _sendPhysical(targetInstanceID, messageDesc, mqt);

   if(MonitorConfig::getInstance()->isEnabled())
   {
       InstanceStats::getInstance()->addToNetSend(messageDesc->getMessageSize());

       std::shared_ptr<scidb::Query> query = Query::getQueryByID(messageDesc->getQueryID(), false);
       if(query) {
            query->getStats().addToNetSend(messageDesc->getMessageSize());
       }
   }
}

void NetworkManager::broadcastPhysical(std::shared_ptr<MessageDesc>& messageDesc)
{
   ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);
   _broadcastPhysical(messageDesc);
}

void NetworkManager::MessageSender::operator() (const InstanceDesc& i)
{
    const InstanceID targetInstanceID = i.getInstanceId();
    if (targetInstanceID != _selfInstanceID) {
        _nm._sendPhysical(targetInstanceID, _messageDesc);
    }
}

bool NetworkManager::_generateConnErrorOnCommitOrAbort(const std::shared_ptr<MessageDesc>& messageDesc)
{
    if (!isDebug()) { return false; }

    SCIDB_ASSERT(_mutex.isLockedByThisThread());

    bool generateError = (messageDesc->getMessageType() == mtAbort ||
                         messageDesc->getMessageType() == mtCommit);
    if (generateError) {
        try {
            InjectedErrorListener::throwif(__LINE__, __FILE__);
            generateError = false;
        } catch (const scidb::Exception& e) {
            generateError = true;
        }
    }
    if (generateError) {
        for (auto& connPair : _outConnections) {
            if (connPair.second) { connPair.second->disconnect(); }
        }
       _outConnections.clear();
    }
    if (generateError) {
        LOG4CXX_WARN(logger, "NetworkManager::_generateConnErrorOnCommitOrAbort: " << "true" );
    }
    return generateError;
}

void NetworkManager::_broadcastPhysical(std::shared_ptr<MessageDesc>& messageDesc)
{
    SCIDB_ASSERT(_mutex.isLockedByThisThread());

    // We are broadcasting to the instances IN THE CURRENT membership,
    // which may be different from the query (of the message) membership.
    // Those who did not participate in the query will drop the message.
    // Those who are no longer in the membership are gone anyway.
    fetchInstances();

    if (!_generateConnErrorOnCommitOrAbort(messageDesc)) {
        MessageSender ms(*this, messageDesc, _selfInstanceID);
        _instanceMembership->visitInstances(ms);
    }
}

void NetworkManager::broadcastLogical(std::shared_ptr<MessageDesc>& messageDesc)
{
    if (!messageDesc->getQueryID().isValid()) {
        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MESSAGE_MISSED_QUERY_ID);
    }
   std::shared_ptr<Query> query = Query::getQueryByID(messageDesc->getQueryID());
   const size_t instancesCount  = query->getInstancesCount();
   InstanceID myInstanceID      = query->getInstanceID();
   assert(instancesCount>0);
   {
      ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);
      for (size_t targetInstanceID = 0; targetInstanceID < instancesCount; ++targetInstanceID)
      {
          if (targetInstanceID != myInstanceID) {
              send(targetInstanceID, messageDesc, query);
          }
      }
   }
}

void NetworkManager::fetchInstances()
{
    ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);

    if (!_instanceMembership) {
        _instanceMembership = Cluster::getInstance()->getInstanceMembership(0);
        _randInstanceIndx = 0;
    }
}

void NetworkManager::fetchLatestInstances()
{
    ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);
    _instanceMembership = Cluster::getInstance()->getInstanceMembership(0);
    _randInstanceIndx = 0;
}

void
NetworkManager::sendLocal(const std::shared_ptr<Query>& query,
                          const std::shared_ptr<MessageDesc>& messageDesc)
{
    const InstanceID physicalId = query->mapLogicalToPhysical(query->getInstanceID());
    messageDesc->setSourceInstanceID(physicalId);
    sendLocal(messageDesc);
}

void
NetworkManager::sendLocal(const std::shared_ptr<MessageDesc>& messageDesc)
{
    // the source instance ID can be any valid instance
    // ??? why should it be anything other than the local instance?
    const InstanceID physicalId = messageDesc->getSourceInstanceID();
    SCIDB_ASSERT(isValidPhysicalInstance(physicalId));

    // NOTICE: the message is dispatched by the current thread (rather than by the networking thread)
    // to guarantee ordering. The current thread should also be prepared to handle any exceptions
    // thrown by the dispatch logic (primarily OverflowException(s) & bad_alloc).
    dispatchMessage(std::shared_ptr<Connection>(), messageDesc);
}

void
NetworkManager::send(InstanceID targetInstanceId,
                     std::shared_ptr<MessageDesc>& msg)
{
   assert(msg);
   if (!msg->getQueryID().isValid()) {
       throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MESSAGE_MISSED_QUERY_ID);
   }
   std::shared_ptr<Query> query = Query::getQueryByID(msg->getQueryID());

   send(targetInstanceId, msg, query);
}

void
NetworkManager::receive(InstanceID sourceInstanceID,
                        std::shared_ptr<MessageDesc>& msg,
                        std::shared_ptr<Query>& query)
{
    Semaphore::ErrorChecker ec = bind(&Query::validate, query);
    ScopedWaitTimer timer(PTW_SWT_NET_RCV);
    query->_receiveSemaphores[sourceInstanceID].enter(ec, PTW_SEM_NET_RCV);
    ScopedMutexLock mutexLock(query->_receiveMutex);
    if (query->_receiveMessages[sourceInstanceID].empty()) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_INSTANCE_OFFLINE) << Iid(sourceInstanceID);
    }
    assert(!query->_receiveMessages[sourceInstanceID].empty());
    msg = query->_receiveMessages[sourceInstanceID].front();
    query->_receiveMessages[sourceInstanceID].pop_front();
}

void NetworkManager::send(InstanceID targetInstanceId,
                          std::shared_ptr<MessageDesc>& msg,
                          std::shared_ptr<Query>& query)
{
    SCIDB_ASSERT(msg);
    SCIDB_ASSERT(query);
    msg->setQueryID(query->getQueryID());
    InstanceID target = query->mapLogicalToPhysical(targetInstanceId);

    ScopedWaitTimer timer(PTW_SWT_NET_SND);
    sendPhysical(target, msg);
}

void NetworkManager::send(InstanceID targetInstanceId,
                          std::shared_ptr<SharedBuffer> const& data,
                          std::shared_ptr< Query> & query)
{
    std::shared_ptr<MessageDesc> msg = make_shared<MessageDesc>(mtBufferSend, data);
    send(targetInstanceId, msg, query);
}

std::shared_ptr<SharedBuffer> NetworkManager::receive(InstanceID sourceInstanceID, std::shared_ptr< Query> & query)
{
    std::shared_ptr<MessageDesc> msg;
    ScopedWaitTimer timer(PTW_SWT_NET_RCV);
    receive(sourceInstanceID, msg, query);
    return msg->getBinary();
}

void NetworkManager::_handleLivenessNotification(std::shared_ptr<const InstanceLiveness>& liveInfo)
{
    if (logger->isDebugEnabled()) {
        MembershipID membId = liveInfo->getMembershipId();
        uint64_t ver = liveInfo->getVersion();

        LOG4CXX_DEBUG(logger, "New liveness information, membershipID=" << membId<<", ver="<<ver);
        for (auto const& i : liveInfo->getDeadInstances()) {
            LOG4CXX_DEBUG(logger, "Dead instanceID=" << Iid(i.getInstanceId())
                          << ", genID=" << i.getGenerationId());
        }
        for (auto const& i : liveInfo->getLiveInstances()) {
            LOG4CXX_DEBUG(logger, "Live instanceID=" << Iid(i.getInstanceId())
                          << ", genID=" << i.getGenerationId());
        }
    }

    if (_shutdown) {
       handleShutdown();
       return;
    }

    ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);

    if (_instanceLiveness &&
        _instanceLiveness->getVersion() == liveInfo->getVersion()) {
       assert(_instanceLiveness->isEqual(*liveInfo));
       return;
    }

    assert(!_instanceLiveness ||
           _instanceLiveness->getVersion() < liveInfo->getVersion());
    _instanceLiveness = liveInfo;

    if (!_instanceMembership ||
        _instanceMembership->getId() != _instanceLiveness->getMembershipId()) {
        SCIDB_ASSERT(_instanceLiveness->getMembershipId() >= _instanceMembership->getId());
        fetchLatestInstances();
    }

    _handleLiveness();
}

void NetworkManager::_handleLiveness()
{
   ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);

   SCIDB_ASSERT(_instanceLiveness);
   SCIDB_ASSERT(_instanceMembership);
   SCIDB_ASSERT(_instanceMembership->getId() >= _instanceLiveness->getMembershipId());

   for (ConnectionMap::iterator connIter = _outConnections.begin();
        connIter != _outConnections.end(); ) {

       const InstanceID id = (*connIter).first;
       bool becauseNotMember(false);

       if (!isDead(id, becauseNotMember)) {
           ++connIter;
           continue;
       }
       std::shared_ptr<Connection>& connection = (*connIter).second;
       if (connection) {
           LOG4CXX_DEBUG(logger, "NetworkManager::_handleLiveness: disconnecting from"
                         << " dead instance "<<id);
           connection->disconnect();
           connection.reset();
       }
       _outConnections.erase(connIter++);
       if (becauseNotMember) { _inConnections.erase(id); }
   }

   if (_instanceLiveness->getMembershipId() != _instanceMembership->getId() ||
       _instanceLiveness->getNumDead() > 0) {
       SCIDB_ASSERT(_instanceLiveness->getMembershipId() <= _instanceMembership->getId());
      _livenessHandleScheduler->schedule();
   }
}

void NetworkManager::_handleAlive(const boost::system::error_code& error)
{
    if (error == boost::asio::error::operation_aborted) {
        LOG4CXX_TRACE(logger, "NetworkManager::_handleAlive: aborted");
        return;
    }
    if (error) {
        LOG4CXX_DEBUG(logger, "NetworkManager::_handleAlive: #"
                      << error << " (" << error.message() << ")");
    }

    std::shared_ptr<MessageDesc> messageDesc = make_shared<MessageDesc>(mtAlive);

    if (_shutdown) {
       handleShutdown();
       LOG4CXX_WARN(logger, "NetworkManager::_handleAlive: shutdown");
       return;
    }

    {
        ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);

        _broadcastPhysical(messageDesc);

        LOG4CXX_TRACE(logger, "NetworkManager::_handleAlive: last timeout="<<_aliveTimeout<<" microsecs"
                      << ", replication msgCount="<<_repMessageCount);

        if (!isBufferSpaceLow()) {
            _aliveTimeout = DEFAULT_ALIVE_TIMEOUT_MICRO;
        } else if (_repMessageCount <= 0 ) {
            _aliveTimeout += CONTROL_MSG_TIME_STEP_MICRO; //+10msec
            // In DEFAULT_ALIVE_TIMEOUT_MICRO / CONTROL_MSG_TIME_STEP_MICRO * CONTROL_MSG_TIMEOUT_MICRO (~= 50 sec)
            // of quiet the timeout will increase from CONTROL_MSG_TIMEOUT_MICRO to DEFAULT_ALIVE_TIMEOUT_MICRO
        }

        if (_aliveTimeout >= DEFAULT_ALIVE_TIMEOUT_MICRO) {
            _aliveTimeout = DEFAULT_ALIVE_TIMEOUT_MICRO;
            _aliveTimer.expires_from_now(posix_time::microseconds(DEFAULT_ALIVE_TIMEOUT_MICRO));
        } else {
            _aliveTimer.expires_from_now(posix_time::microseconds(CONTROL_MSG_TIMEOUT_MICRO));
        }

        _aliveTimer.async_wait(NetworkManager::handleAlive);
    }

    // Mutex not held for this.
    AuthMessageHandleJob::slowAuthKiller();
}

static void abortQueryOnConnError(const InstanceID remoteInstanceId,
                                  const std::shared_ptr<Query>& query,
                                  const std::shared_ptr<Exception>& excPtr)
{
    SCIDB_ASSERT(query);
    SCIDB_ASSERT(excPtr);

    const bool isAbort = isValidPhysicalInstance(remoteInstanceId) &&
                         (remoteInstanceId == query->getPhysicalCoordinatorID());
    if (isAbort) {
        std::shared_ptr<MessageDesc> msg = makeAbortMessage(query->getQueryID());
        // HACK (somewhat): set sourceid to coordinator, because only it can issue an abort
        msg->setSourceInstanceID(remoteInstanceId);
        NetworkManager::getInstance()->sendLocal(msg);
    } else {
        arena::ScopedArenaTLS arenaTLS(query->getArena());
        query->handleError(excPtr);
    }
}

void NetworkManager::reconnect(InstanceID instanceId)
{
    bool isRemoteInstanceLive = false;

    {
        ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);

        ConnectionMap::iterator connIter = _outConnections.find(instanceId);
        if (connIter != _outConnections.end()) {

            SCIDB_ASSERT((*connIter).first == instanceId);

            std::shared_ptr<Connection>& connection = (*connIter).second;

            if (connection) {
                SCIDB_ASSERT(isValidPhysicalInstance(instanceId));

                LOG4CXX_DEBUG(logger, "NetworkManager::reconnect: disconnecting from "
                              << Iid(instanceId));
                connection->disconnect();
                connection.reset();

                bool dummy(false);
                isRemoteInstanceLive = (!isDead(instanceId, dummy));
            }
            _outConnections.erase(connIter);
        }
        // the connection will be restarted on-demand
    }

    if (isRemoteInstanceLive) {
        handleConnectionErrorToLive(instanceId);
    }
}

/**
 * NetworkManager needs to schedule a cancel job which injects a query arena to the thread-local storage.
 * Query::handleCancel asserts the query arena was already in the thread-local storage.
 * This routine fills in the gap.
 */
void setArenaTLSAndCancel(const std::shared_ptr<Query>& query)
{
    arena::ScopedArenaTLS arenaTLS(query->getArena());
    query->handleCancel();
}


void NetworkManager::handleClientDisconnect(const QueryID& queryId,
                                            const ClientContext::DisconnectHandler& dh)
{
    if (!queryId.isValid()) {
      return;
   }

   LOG4CXX_DEBUG(logger, str(format("Client for query %lld disconnected") % queryId));
   std::shared_ptr<Query> query = Query::getQueryByID(queryId, false);
   if (!query) {
       LOG4CXX_TRACE(logger, __PRETTY_FUNCTION__ << ": Query " << queryId
                     << " not found, nothing to do");
       return;
   }

   arena::ScopedArenaTLS arenaTLS(query->getArena());
   if (!dh) {
       assert(query->isCoordinator());
       std::shared_ptr<scidb::WorkQueue> errorQ = query->getErrorQueue();

       if (!errorQ) {
           LOG4CXX_TRACE(logger, "Query " << query->getQueryID()
                         << " no longer has the queue for error reporting,"
                         " it must be no longer active");
           return;
       }

       WorkQueue::WorkItem item = boost::bind(&setArenaTLSAndCancel, query);
       boost::function<void()> work = boost::bind(&WorkQueue::enqueue, errorQ, item);
       item.clear();
       // XXX TODO: handleCancel() sends messages, and stalling the network thread can theoretically
       // cause a deadlock when throttle control is enabled. So, when it is enabled,
       // we can handle the throttle-control exceptions in handleCancel() to avoid the dealock
       // (independently of this code).
       Query::runRestartableWork<void, WorkQueue::OverflowException>(work);

   } else {
       WorkQueue::WorkItem item = boost::bind(dh, query);
       try {
           _workQueue->enqueue(item);
       } catch (const WorkQueue::OverflowException& e) {
           LOG4CXX_ERROR(logger, "Overflow exception from the work queue: "<<e.what());
           assert(false);
           query->handleError(e.copy());
       }
   }
}

namespace {
    void runSessionCallback(pthread_t netmgrTid, Session::Callback cb)
    {
        // Must *not* run in network manager thread!
        SCIDB_ASSERT(!::pthread_equal(netmgrTid, ::pthread_self()));
        cb();
    }
}

void NetworkManager::handleSessionClose(std::shared_ptr<Session>& sessp)
{
    // Run self-contained session close callback in another thread.
    Session::Callback cb = sessp->swapCleanup(Session::Callback());
    SCIDB_ASSERT(cb);
    WorkQueue::WorkItem item =
        boost::bind(&runSessionCallback, _selfThreadID, cb);
    _requestQueue->enqueue(item);
}

void NetworkManager::handleConnectionError(const InstanceID remoteInstanceId,
                                           const QueryID& queryID,
                                           const boost::system::error_code& error)
{
    if (!queryID.isValid()) {
        return;
    }
    LOG4CXX_ERROR(logger, "NetworkManager::handleConnectionError: Post error #"
                  << error << " (" << error.message() << ") to query " << queryID);

    std::shared_ptr<Query> query = Query::getQueryByID(queryID, false);
    if (!query) {
        LOG4CXX_TRACE(logger, __PRETTY_FUNCTION__ << ": Query " << queryID
                      << " not found, nothing to do");
        return;
    }

    std::shared_ptr<SystemException> excPtr =
            SYSTEM_EXCEPTION_SPTR(SCIDB_SE_NETWORK, SCIDB_LE_CONNECTION_ERROR2);
    *excPtr << error << error.message();

    abortQueryOnConnError(remoteInstanceId, query, excPtr);
}

void NetworkManager::handleConnectionError(const InstanceID remoteInstanceId,
                                           const QueryID& queryID,
                                           const Exception& ex)
{
    if (!queryID.isValid()) {
        return;
    }
    LOG4CXX_ERROR(logger, "NetworkManager::handleConnectionError: Post exception \""
                  << ex.what() << "\" to query " << queryID);

    std::shared_ptr<Query> query = Query::getQueryByID(queryID, false);
    if (!query) {
        LOG4CXX_TRACE(logger, __PRETTY_FUNCTION__ << ": Query " << queryID
                      << " not found, nothing to do");
        return;
   }

   abortQueryOnConnError(remoteInstanceId, query, ex.copy());
}

bool NetworkManager::isDead(InstanceID remoteInstanceId, bool& becauseNotMember)
{
    ScopedMutexLock mutexLock(_mutex);

    SCIDB_ASSERT(remoteInstanceId != _selfInstanceID);

    fetchInstances();

    if (_instanceLiveness) {

        SCIDB_ASSERT(_instanceLiveness->getMembershipId() <= _instanceMembership->getId());
        // if remoteInstanceId is no longer in the membership,
        // we treat it as dead

        return  (_instanceLiveness->isDead(remoteInstanceId) ||
                (_instanceMembership->getId() != _instanceLiveness->getMembershipId() &&
                 (becauseNotMember = !_instanceMembership->isMember(remoteInstanceId))));
    }
    return (becauseNotMember = !_instanceMembership->isMember(remoteInstanceId));
}

void
NetworkManager::handleConnectionError(const InstanceID remoteInstanceId,
                                      const set<QueryID>& queries,
                                      const boost::system::error_code& error)
{
    for (QueryID const& qid : queries) {
        handleConnectionError(remoteInstanceId, qid, error);
    }
    return;
}

void
NetworkManager::handleConnectionErrorToLive(const InstanceID remoteInstanceId)
{
    SCIDB_ASSERT(isValidPhysicalInstance(remoteInstanceId));

    // When a TCP connection error occurs, there might be some messages
    // in flight which have already left our queues, so there is no
    // reliable way to detect which queries are affected without some
    // registration mechanism. Such a mechanism seems an overkill
    // at this point, so just abort all the queries with the following
    // two exceptions:
    // 1. For a client connection it does not matter because
    // all the queries attached to that connection will be aborted.
    // This method is not invoked for the client connections.
    // 2. For a "dead" instance, we should not abort all the queries
    // because the failed attempts to connect to that instance may
    // abort the queries not using the dead instance.
    // The queries which include remoteInstanceId in their live sets
    // are supposed to be notified via the liveness mechanism.

    // NOTE: remoteInstaceId may be dead by now and some queries (not using it)
    // may get aborted unnecessarily - life is tough, it is a race anyway

    LOG4CXX_ERROR(logger, "NetworkManager::handleConnectionError: "
                  "Connection error - aborting ALL queries");

    std::shared_ptr<SystemException> excPtr =
            SYSTEM_EXCEPTION_SPTR(SCIDB_SE_NETWORK, SCIDB_LE_CONNECTION_ERROR2);
    *excPtr << "(unknown)" << "possible connection state loss";

    size_t qNum = Query::visitQueries(
            Query::Visitor(
                    boost::bind(&abortQueryOnConnError, remoteInstanceId, _1, excPtr)));

    LOG4CXX_TRACE(logger, "NetworkManager::handleConnectionError: "
                  "Aborted " << qNum << " queries");

    dispatchErrorToListener(remoteInstanceId);
}

namespace {
// DisconnectMessageDesc needs to be a separate class, at this point in time.  This
// MessageDesc derived class is turned into a "MessageDescription" (yes, that is a
// different class) so that Subscribers to Notification<MessageDescription> will be
// notified.  The problem is that "mtNone" (which is the only used messageType for
// DisconnectMessageDesc) is invalid in the base class because `bool validate()` will
// throw an exception. So this class is needed to override the methods: 'createRecord' and
// 'validate'.
class DisconnectMessageDesc : public MessageDesc
{
public:
    DisconnectMessageDesc() {}
    virtual ~DisconnectMessageDesc() {}
    virtual bool validate() { return true; }
protected:
    virtual MessagePtr createRecord(MessageID messageType) override
    {
        return MessagePtr(new scidb_msg::DummyQuery());
    }
private:
    DisconnectMessageDesc(const DisconnectMessageDesc&) = delete ;
    DisconnectMessageDesc& operator=(const DisconnectMessageDesc&) = delete ;
};
}

void
NetworkManager::dispatchErrorToListener(InstanceID remoteInstanceId)
{
    SCIDB_ASSERT(!_mutex.isLockedByThisThread());
    SCIDB_ASSERT(isValidPhysicalInstance(remoteInstanceId));

    NetworkMessageFactory::MessageHandler handler =
            boost::bind(&NetworkManager::publishMessage, _1);

    std::shared_ptr<MessageDesc> msgDesc = std::make_shared<DisconnectMessageDesc>();
    msgDesc->setSourceInstanceID(remoteInstanceId);
    msgDesc->initRecord(mtNone); // Here mtNone indicates error condition.
    dispatchMessageToListener(std::shared_ptr<Connection>(),
                              msgDesc, handler);
}

void Send(void* ctx, InstanceID instance, void const* data, size_t size)
{
    std::shared_ptr<SharedBuffer> buf(make_shared<MemoryBuffer>(data, size));
    NetworkManager::getInstance()->send(instance, buf,
                                        *(std::shared_ptr<Query>*)ctx);
}

void Receive(void* ctx, InstanceID instance, void* data, size_t size)
{
    NetworkManager* nm = NetworkManager::getInstance();
    SCIDB_ASSERT(nm);
    std::shared_ptr<SharedBuffer> buf = nm->receive(instance,
                                               *(std::shared_ptr<Query>*)ctx);
    SCIDB_ASSERT(buf->getSize() == size);
    memcpy(data, buf->getConstData(), buf->getSize());
}

void BufSend(InstanceID target, std::shared_ptr<SharedBuffer> const& data, std::shared_ptr<Query>& query)
{
    NetworkManager::getInstance()->send(target, data, query);
}

std::shared_ptr<SharedBuffer> BufReceive(InstanceID source, std::shared_ptr<Query>& query)
{
    return NetworkManager::getInstance()->receive(source,query);
}

void BufBroadcast(std::shared_ptr<SharedBuffer> const& data, std::shared_ptr<Query>& query)
{
    std::shared_ptr<MessageDesc> msg = make_shared<MessageDesc>(mtBufferSend, data);
    msg->setQueryID(query->getQueryID());
    NetworkManager::getInstance()->broadcastLogical(msg);
}

bool
NetworkManager::DefaultNetworkMessageFactory::isRegistered(const MessageID& msgID)
{
   ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);
   return (_msgHandlers.find(msgID) != _msgHandlers.end());
}

bool
NetworkManager::DefaultNetworkMessageFactory::addMessageType(const MessageID& msgID,
                                                             const MessageCreator& msgCreator,
                                                             const MessageHandler& msgHandler)
{
   if (!isPluginMessage(msgID)) {
      return false;
   }
   ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);
   return  _msgHandlers.insert(
              std::make_pair(msgID,
                 std::make_pair(msgCreator, msgHandler))).second;
}

bool
NetworkManager::DefaultNetworkMessageFactory::removeMessageType(const MessageID& msgId)
{
   if (!isPluginMessage(msgId)) {
      return false;
   }
   ScopedMutexLock mutexLock(_mutex);
   return (_msgHandlers.erase(msgId) > 0);
}


MessagePtr
NetworkManager::DefaultNetworkMessageFactory::createMessage(const MessageID& msgID)
{
   MessagePtr msgPtr;
   NetworkMessageFactory::MessageCreator creator;
   {
      ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);
      MessageHandlerMap::const_iterator iter = _msgHandlers.find(msgID);
      if (iter != _msgHandlers.end()) {
         creator = iter->second.first;
      }
   }
   if (!creator.empty()) {
      msgPtr = creator(msgID);
   }
   return msgPtr;
}

NetworkMessageFactory::MessageHandler
NetworkManager::DefaultNetworkMessageFactory::getMessageHandler(const MessageID& msgType)
{
   ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);

   MessageHandlerMap::const_iterator iter = _msgHandlers.find(msgType);
   if (iter != _msgHandlers.end()) {
      NetworkMessageFactory::MessageHandler handler = iter->second.second;
      return handler;
   }
   NetworkMessageFactory::MessageHandler emptyHandler;
   return emptyHandler;
}

/**
 * @see Network.h
 */
std::shared_ptr<NetworkMessageFactory> getNetworkMessageFactory()
{
   return NetworkManager::getInstance()->getNetworkMessageFactory();
}

/**
 * @see Network.h
 */
boost::asio::io_service& getIOService()
{
   return NetworkManager::getInstance()->getIOService();
}

std::shared_ptr<MessageDesc> prepareMessage(MessageID msgID,
                                            MessagePtr record,
                                            boost::asio::const_buffer& binary)
{
   std::shared_ptr<SharedBuffer> payload;
   if (boost::asio::buffer_size(binary) > 0) {
      assert(boost::asio::buffer_cast<const void*>(binary));
      payload = std::shared_ptr<SharedBuffer>(new MemoryBuffer(boost::asio::buffer_cast<const void*>(binary),
                                                          boost::asio::buffer_size(binary)));
   }
   std::shared_ptr<MessageDesc> msgDesc =
           std::make_shared<Connection::ServerMessageDesc>(payload);

   msgDesc->initRecord(msgID);
   MessagePtr msgRecord = msgDesc->getRecord<Message>();
   const google::protobuf::Descriptor* d1 = msgRecord->GetDescriptor();
   assert(d1);
   const google::protobuf::Descriptor* d2 = record->GetDescriptor();
   assert(d2);
   if (d1->full_name().compare(d2->full_name()) != 0) {
      throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_MESSAGE_TYPE);
   }
   msgRecord->CopyFrom(*record.get());

   return msgDesc;
}

/**
 * @see Network.h
 */
void sendAsyncPhysical(InstanceID targetInstanceID,
               MessageID msgID,
               MessagePtr record,
               boost::asio::const_buffer& binary)
{
   std::shared_ptr<MessageDesc> msgDesc = prepareMessage(msgID,record,binary);
   assert(msgDesc);
   NetworkManager::getInstance()->sendPhysical(targetInstanceID, msgDesc);
}

/**
 * @see Network.h
 */
void sendAsyncClient(ClientContext::Ptr& clientCtx,
               MessageID msgID,
               MessagePtr record,
               boost::asio::const_buffer& binary)
{
    Connection* conn = dynamic_cast<Connection*>(clientCtx.get());
    if (conn == NULL) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_CTX)
               << typeid(*clientCtx).name());
    }
    std::shared_ptr<MessageDesc> msgDesc = prepareMessage(msgID,record,binary);
    assert(msgDesc);
    conn->sendMessage(msgDesc);
}

std::shared_ptr<WorkQueue> getWorkQueue()
{
    return NetworkManager::getInstance()->getWorkQueue();
}

uint32_t getLivenessTimeout()
{
   return Config::getInstance()->getOption<int>(CONFIG_LIVENESS_TIMEOUT);
}

std::shared_ptr<Scheduler> getScheduler(Scheduler::Work& workItem, time_t period)
{
   if (!workItem) {
      throw USER_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_INVALID_SHEDULER_WORK_ITEM);
   }
   if (period < 1) {
      throw USER_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_INVALID_SHEDULER_PERIOD);
   }
   std::shared_ptr<scidb::Scheduler> scheduler(new ThrottledScheduler(period, workItem,
                                          NetworkManager::getInstance()->getIOService()));
   return scheduler;
}

void resolveComplete(std::shared_ptr<asio::ip::tcp::resolver>& resolver,
                     std::shared_ptr<asio::ip::tcp::resolver::query>& query,
                     ResolverFunc& cb,
                     const system::error_code& error,
                     asio::ip::tcp::resolver::iterator endpoint_iterator)
{
    try {
        cb(error, endpoint_iterator);
    } catch (const scidb::Exception& e) {
        LOG4CXX_ERROR(logger, "Name resolution callback failed with: "<<e.what());
        assert(false);
    }
}

void resolveAsync(const string& address, const string& service, ResolverFunc& cb)
{
    std::shared_ptr<asio::ip::tcp::resolver> resolver(
        new asio::ip::tcp::resolver(NetworkManager::getInstance()->getIOService()));
    std::shared_ptr<asio::ip::tcp::resolver::query> query =
       make_shared<asio::ip::tcp::resolver::query>(address, service);
    resolver->async_resolve(*query,
                            boost::bind(&scidb::resolveComplete,
                                        resolver, query, cb,
                                        boost::asio::placeholders::error,
                                        boost::asio::placeholders::iterator));
}

} // namespace scidb
